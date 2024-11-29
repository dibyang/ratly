package net.xdob.ratly.server.raftlog.segmented;

import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.metrics.Timekeeper;
import net.xdob.ratly.protocol.RaftGroupMemberId;
import net.xdob.ratly.server.Division;
import net.xdob.ratly.server.config.Log;
import net.xdob.ratly.server.metrics.SegmentedRaftLogMetrics;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.LogEntryHeader;
import net.xdob.ratly.server.raftlog.LogProtoUtils;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.raftlog.RaftLogBase;
import net.xdob.ratly.server.raftlog.RaftLogIOException;
import net.xdob.ratly.server.storage.RaftStorageMetadata;
import net.xdob.ratly.server.storage.RaftStorage;
import net.xdob.ratly.server.raftlog.segmented.LogSegment.LogRecord;
import net.xdob.ratly.server.raftlog.segmented.SegmentedRaftLogCache.TruncateIndices;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.statemachine.StateMachine;
import net.xdob.ratly.statemachine.TransactionContext;
import net.xdob.ratly.statemachine.impl.TransactionContextImpl;
import com.google.protobuf.ByteString;
import net.xdob.ratly.util.AutoCloseableLock;
import net.xdob.ratly.util.AwaitToRun;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.ReferenceCountedObject;
import net.xdob.ratly.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import net.xdob.ratly.util.UncheckedAutoCloseable;

/**
 *
 * 这是一个将日志条目写入本地磁盘分段文件的 RaftLog 实现。
 * <p>
 * 最大日志段大小为 8MB。实际的日志段大小可能不会完全等于此限制。如果一个日志条目的大小超过 8MB，
 * 则该条目将存储在一个单独的段中。
 * <p>
 * 日志存储分为两种段：
 *    1.已完成的日志段，命名为“log_startindex-endindex”，不可再追加日志，但可被截断。
 *    2.当前正在写入的日志段，命名为“log_inprogress_startindex”。
 * <p>
 * 可以有多个已关闭的日志段，但最多只能有一个开放的日志段。
 * 当开放的日志段达到大小限制，或者日志的任期增加时，
 * 我们会关闭当前的开放段并启动一个新的开放段。
 * 已关闭的日志段不能再追加内容，但如果某个跟随者的日志与当前领导者不一致，可以对其进行截断。
 * <p>
 * 每个已关闭的日志段应该是非空的，即至少包含一个条目。
 * <p>
 * 段与段之间不应有任何间隙。第一个段可能不会从索引 0 开始，因为可能存在作为日志压缩的快照。
 * 段中的最后一个索引应该不小于快照的最后一个索引，否则在进一步追加日志时可能会产生空洞。
 */
public final class SegmentedRaftLog extends RaftLogBase {
  /**
   * I/O task definitions.
   */
  abstract static class Task {
    private final CompletableFuture<Long> future = new CompletableFuture<>();
    private Timekeeper.Context queueTimerContext;

    CompletableFuture<Long> getFuture() {
      return future;
    }

    void done() {
      completeFuture();
    }

    void discard() {
    }

    final void completeFuture() {
      final boolean completed = future.complete(getEndIndex());
      Preconditions.assertTrue(completed,
          () -> this + " is already " + StringUtils.completableFuture2String(future, false));
    }

    void failed(IOException e) {
      this.getFuture().completeExceptionally(e);
    }

    abstract void execute() throws IOException;

    abstract long getEndIndex();

    void startTimerOnEnqueue(Timekeeper queueTimer) {
      queueTimerContext = queueTimer.time();
    }

    void stopTimerOnDequeue() {
      if (queueTimerContext != null) {
        queueTimerContext.stop();
      }
    }

    int getSerializedSize() {
      return 0;
    }

    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass()) + ":" + getEndIndex();
    }
  }

  /**
   * 在 {@link SegmentedRaftLog} 中使用的服务器方法。
   */
  interface ServerLogMethods {
    ServerLogMethods DUMMY = new ServerLogMethods() {};

    default long[] getFollowerNextIndices() {
      return null;
    }

    default long getLastAppliedIndex() {
      return INVALID_LOG_INDEX;
    }

    /** 通知服务器日志条目正在被截断。 */
    default void notifyTruncatedLogEntry(TermIndex ti) {
    }

    default TransactionContext getTransactionContext(LogEntryProto entry, boolean createNew) {
      return null;
    }
  }

  /**
   * 当服务器为 null 时，返回 {@link ServerLogMethods} 的虚拟实例。
   * 否则，服务器非 null，返回使用给定服务器的实现。
   */
  private ServerLogMethods newServerLogMethods(Division impl,
                                               Consumer<LogEntryProto> notifyTruncatedLogEntry,
                                               BiFunction<LogEntryProto, Boolean, TransactionContext> getTransactionContext) {
    if (impl == null) {
      return ServerLogMethods.DUMMY;
    }

    return new ServerLogMethods() {
      @Override
      public long[] getFollowerNextIndices() {
        return impl.getInfo().getFollowerNextIndices();
      }

      @Override
      public long getLastAppliedIndex() {
        return impl.getInfo().getLastAppliedIndex();
      }

      @Override
      public void notifyTruncatedLogEntry(TermIndex ti) {
        ReferenceCountedObject<LogEntryProto> ref = null;
        try {
          ref = retainLog(ti.getIndex());
          final LogEntryProto entry = ref != null ? ref.get() : null;
          notifyTruncatedLogEntry.accept(entry);
        } catch (RaftLogIOException e) {
          LOG.error("{}: Failed to read log {}", getName(), ti, e);
        } finally {
          if (ref != null) {
            ref.release();
          }
        }
      }

      @Override
      public TransactionContext getTransactionContext(LogEntryProto entry, boolean createNew) {
        return getTransactionContext.apply(entry, createNew);
      }
    };
  }

  private final ServerLogMethods server;
  private final RaftStorage storage;
  private final StateMachine stateMachine;
  private final SegmentedRaftLogCache cache;
  private final AwaitToRun cacheEviction;
  private final SegmentedRaftLogWorker fileLogWorker;
  private final long segmentMaxSize;
  private final boolean stateMachineCachingEnabled;
  private final SegmentedRaftLogMetrics metrics;

  @SuppressWarnings({"squid:S2095"}) // Suppress closeable  warning
  private SegmentedRaftLog(Builder b) {
    super(b.memberId, b.snapshotIndexSupplier, b.properties);
    this.metrics = new SegmentedRaftLogMetrics(b.memberId);

    this.server = newServerLogMethods(b.server, b.notifyTruncatedLogEntry, b.getTransactionContext);
    this.storage = b.storage;
    this.stateMachine = b.stateMachine;
    this.segmentMaxSize = Log.segmentSizeMax(b.properties).getSize();
    this.cache = new SegmentedRaftLogCache(b.memberId, storage, b.properties, getRaftLogMetrics());
    this.cacheEviction = new AwaitToRun(b.memberId + "-cacheEviction", this::checkAndEvictCache).start();
    this.fileLogWorker = new SegmentedRaftLogWorker(b.memberId, stateMachine,
        b.submitUpdateCommitEvent, b.server, storage, b.properties, getRaftLogMetrics());
    stateMachineCachingEnabled = Log.StateMachineData.cachingEnabled(b.properties);
  }

  @Override
  public SegmentedRaftLogMetrics getRaftLogMetrics() {
    return metrics;
  }

  @Override
  protected void openImpl(long lastIndexInSnapshot, Consumer<LogEntryProto> consumer) throws IOException {
    loadLogSegments(lastIndexInSnapshot, consumer);
    final File openSegmentFile = Optional.ofNullable(cache.getOpenSegment()).map(LogSegment::getFile).orElse(null);
    fileLogWorker.start(Math.max(cache.getEndIndex(), lastIndexInSnapshot),
        Math.min(cache.getLastIndexInClosedSegments(), lastIndexInSnapshot),
        openSegmentFile);
  }

  @Override
  public long getStartIndex() {
    return cache.getStartIndex();
  }

  private void loadLogSegments(long lastIndexInSnapshot,
      Consumer<LogEntryProto> logConsumer) throws IOException {
    try(AutoCloseableLock writeLock = writeLock()) {
      final List<LogSegmentPath> paths = LogSegmentPath.getLogSegmentPaths(storage);
      int i = 0;
      for (LogSegmentPath pi : paths) {
        // 在初始加载过程中，我们只能基于快照确认已提交的索引。
        // 这意味着，如果日志段在初始加载后没有保留在缓存中，之后我们必须重新加载其内容以更新状态机。
        // TODO：我们应该让 Raft 节点定期持久化其已提交的索引，
        //  这样在初始加载时，我们可以将部分日志条目应用到状态机中。
        boolean keepEntryInCache = (paths.size() - i++) <= cache.getMaxCachedSegments();
        try(UncheckedAutoCloseable ignored = getRaftLogMetrics().startLoadSegmentTimer()) {
          cache.loadSegment(pi, keepEntryInCache, logConsumer);
        }
      }

      // 如果最大索引小于快照中的最后一个索引，我们将不会加载日志，以避免日志段之间出现空洞。
      // 这种情况可能发生在本地 I/O 工作线程持久化日志过慢（比提交日志和生成快照的速度还慢）时。
      if (!cache.isEmpty() && cache.getEndIndex() < lastIndexInSnapshot) {
        LOG.warn("End log index {} is smaller than last index in snapshot {}",
            cache.getEndIndex(), lastIndexInSnapshot);
        purgeImpl(lastIndexInSnapshot).whenComplete((purged, e) -> updatePurgeIndex(purged));
      }
    }
  }

  @Override
  public LogEntryProto get(long index) throws RaftLogIOException {
    final ReferenceCountedObject<LogEntryProto> ref = retainLog(index);
    if (ref == null) {
      return null;
    }
    try {
      return LogProtoUtils.copy(ref.get());
    } finally {
      ref.release();
    }
  }

  @Override
  public ReferenceCountedObject<LogEntryProto> retainLog(long index) throws RaftLogIOException {
    checkLogState();
    final LogSegment segment = cache.getSegment(index);
    if (segment == null) {
      return null;
    }
    final LogRecord record = segment.getLogRecord(index);
    if (record == null) {
      return null;
    }
    final ReferenceCountedObject<LogEntryProto> entry = segment.getEntryFromCache(record.getTermIndex());
    if (entry != null) {
      try {
        entry.retain();
        getRaftLogMetrics().onRaftLogCacheHit();
        return entry;
      } catch (IllegalStateException ignored) {
        // 条目可以从缓存中移除并释放。
        // 这个异常可以安全地忽略，因为它与缓存未命中的情况相同。
      }
    }
    //// 条目不在段的缓存中。在不持有锁的情况下加载缓存。
    getRaftLogMetrics().onRaftLogCacheMiss();
    cacheEviction.signal();
    return segment.loadCache(record);
  }

  @Override
  public EntryWithData getEntryWithData(long index) throws RaftLogIOException {
    throw new UnsupportedOperationException("Use retainEntryWithData(" + index + ") instead.");
  }

  @Override
  public ReferenceCountedObject<EntryWithData> retainEntryWithData(long index) throws RaftLogIOException {
    final ReferenceCountedObject<LogEntryProto> entryRef = retainLog(index);
    if (entryRef == null) {
      throw new RaftLogIOException("Log entry not found: index = " + index);
    }

    final LogEntryProto entry = entryRef.get();
    if (!LogProtoUtils.isStateMachineDataEmpty(entry)) {
      return newEntryWithData(entryRef);
    }

    try {
      CompletableFuture<ReferenceCountedObject<ByteString>> future = null;
      if (stateMachine != null) {
        future = stateMachine.data().retainRead(entry, server.getTransactionContext(entry, false)).exceptionally(ex -> {
          stateMachine.event().notifyLogFailed(ex, entry);
          throw new CompletionException("Failed to read state machine data for log entry " + entry, ex);
        });
      }
      return future != null? newEntryWithData(entryRef, future): newEntryWithData(entryRef);
    } catch (Exception e) {
      final String err = getName() + ": Failed readStateMachineData for " + toLogEntryString(entry);
      LOG.error(err, e);
      entryRef.release();
      throw new RaftLogIOException(err, JavaUtils.unwrapCompletionException(e));
    }
  }

  private void checkAndEvictCache() {
    if (cache.shouldEvict()) {
      try (AutoCloseableLock ignored = writeLock()){
        // TODO：如果缓存达到了最大大小且无法驱逐任何段的缓存，应阻止新条目的追加或新段的分配。
        cache.evictCache(server.getFollowerNextIndices(), fileLogWorker.getSafeCacheEvictIndex(),
            server.getLastAppliedIndex());
      }
    }
  }

  @Override
  public TermIndex getTermIndex(long index) {
    checkLogState();
    return cache.getTermIndex(index);
  }

  @Override
  public LogEntryHeader[] getEntries(long startIndex, long endIndex) {
    checkLogState();
    return cache.getTermIndices(startIndex, endIndex);
  }

  @Override
  public TermIndex getLastEntryTermIndex() {
    checkLogState();
    return cache.getLastTermIndex();
  }

  @Override
  protected CompletableFuture<Long> truncateImpl(long index) {
    checkLogState();
    try(AutoCloseableLock writeLock = writeLock()) {
      SegmentedRaftLogCache.TruncationSegments ts = cache.truncate(index);
      if (ts != null) {
        Task task = fileLogWorker.truncate(ts, index);
        return task.getFuture();
      }
    }
    return CompletableFuture.completedFuture(index);
  }


  @Override
  protected CompletableFuture<Long> purgeImpl(long index) {
    try (AutoCloseableLock writeLock = writeLock()) {
      SegmentedRaftLogCache.TruncationSegments ts = cache.purge(index);
      updateSnapshotIndexFromStateMachine();
      if (ts != null) {
        LOG.info("{}: {}", getName(), ts);
        Task task = fileLogWorker.purge(ts);
        return task.getFuture();
      }
    }
    LOG.debug("{}: purge({}) found nothing to purge.", getName(), index);
    return CompletableFuture.completedFuture(index);
  }

  @Override
  protected CompletableFuture<Long> appendEntryImpl(ReferenceCountedObject<LogEntryProto> entryRef,
      TransactionContext context) {
    checkLogState();
    LogEntryProto entry = entryRef.retain();
    if (LOG.isTraceEnabled()) {
      LOG.trace("{}: appendEntry {}", getName(), LogProtoUtils.toLogEntryString(entry));
    }
    final LogEntryProto removedStateMachineData = LogProtoUtils.removeStateMachineData(entry);
    try(AutoCloseableLock writeLock = writeLock()) {
      final Timekeeper.Context appendEntryTimerContext = getRaftLogMetrics().startAppendEntryTimer();
      validateLogEntry(entry);
      final LogSegment currentOpenSegment = cache.getOpenSegment();
      boolean rollOpenSegment = false;
      if (currentOpenSegment == null) {
        cache.addOpenSegment(entry.getIndex());
        fileLogWorker.startLogSegment(entry.getIndex());
      } else if (isSegmentFull(currentOpenSegment, removedStateMachineData)) {
        rollOpenSegment = true;
      } else {
        final TermIndex last = currentOpenSegment.getLastTermIndex();
        if (last != null && last.getTerm() != entry.getTerm()) {
          // the term changes
          Preconditions.assertTrue(last.getTerm() < entry.getTerm(),
              "open segment's term %s is larger than the new entry's term %s",
              last.getTerm(), entry.getTerm());
          rollOpenSegment = true;
        }
      }

      if (rollOpenSegment) {
        cache.rollOpenSegment(true);
        fileLogWorker.rollLogSegment(currentOpenSegment);
        cacheEviction.signal();
      }

      // 如果条目包含状态机数据，则应首先将该条目插入到状态机中，然后再插入到缓存中。
      // 未按照此顺序操作会导致缓存中留下一个无效条目。
      final Task write = fileLogWorker.writeLogEntry(entryRef, removedStateMachineData, context);
      if (stateMachineCachingEnabled) {
        // 状态机数据将被缓存到状态机内部。
        if (removedStateMachineData != entry) {
          cache.appendEntry(LogSegment.Op.WRITE_CACHE_WITH_STATE_MACHINE_CACHE,
              ReferenceCountedObject.wrap(removedStateMachineData));
        } else {
          cache.appendEntry(LogSegment.Op.WRITE_CACHE_WITH_STATE_MACHINE_CACHE,
              ReferenceCountedObject.wrap(LogProtoUtils.copy(removedStateMachineData)));
        }
      } else {
        cache.appendEntry(LogSegment.Op.WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE, entryRef);
      }
      return write.getFuture().whenComplete((clientReply, exception) -> appendEntryTimerContext.stop());
    } catch (Exception e) {
      LOG.error("{}: Failed to append {}", getName(), toLogEntryString(entry), e);
      throw e;
    } finally {
      entryRef.release();
    }
  }

  private boolean isSegmentFull(LogSegment segment, LogEntryProto entry) {
    if (segment.getTotalFileSize() >= segmentMaxSize) {
      return true;
    } else {
      final long entrySize = LogSegment.getEntrySize(entry, LogSegment.Op.CHECK_SEGMENT_FILE_FULL);
      // 如果条目大小大于最大段大小，则直接写入当前段。
      return entrySize <= segmentMaxSize &&
          segment.getTotalFileSize() + entrySize > segmentMaxSize;
    }
  }

  @Override
  protected List<CompletableFuture<Long>> appendImpl(ReferenceCountedObject<List<LogEntryProto>> entriesRef) {
    checkLogState();
    final List<LogEntryProto> entries = entriesRef.retain();
    if (entries == null || entries.isEmpty()) {
      entriesRef.release();
      return Collections.emptyList();
    }
    try (AutoCloseableLock writeLock = writeLock()) {
      final TruncateIndices ti = cache.computeTruncateIndices(server::notifyTruncatedLogEntry, entries);
      final long truncateIndex = ti.getTruncateIndex();
      final int index = ti.getArrayIndex();
      LOG.debug("truncateIndex={}, arrayIndex={}", truncateIndex, index);

      final List<CompletableFuture<Long>> futures;
      if (truncateIndex != -1) {
        futures = new ArrayList<>(entries.size() - index + 1);
        futures.add(truncate(truncateIndex));
      } else {
        futures = new ArrayList<>(entries.size() - index);
      }
      for (int i = index; i < entries.size(); i++) {
        final LogEntryProto entry = entries.get(i);
        TransactionContextImpl transactionContext = (TransactionContextImpl) server.getTransactionContext(entry, true);
        futures.add(appendEntry(entriesRef.delegate(entry), transactionContext));
      }
      return futures;
    } finally {
      entriesRef.release();
    }
  }


  @Override
  public long getFlushIndex() {
    return fileLogWorker.getFlushIndex();
  }

  @Override
  public void persistMetadata(RaftStorageMetadata metadata) throws IOException {
    storage.getMetadataFile().persist(metadata);
  }

  @Override
  public RaftStorageMetadata loadMetadata() throws IOException {
    return storage.getMetadataFile().getMetadata();
  }

  @Override
  public CompletableFuture<Long> onSnapshotInstalled(long lastSnapshotIndex) {
    updateSnapshotIndex(lastSnapshotIndex);
    fileLogWorker.syncWithSnapshot(lastSnapshotIndex);
    // TODO：清理正常/临时/损坏的快照文件。
    // 如果快照中的最后一个索引大于最后一个日志条目的索引，我们应该删除所有日志条目及其缓存，以避免日志段之间的间隙。

    // 如果条目已经包含在快照中，关闭打开的日志段。
    LogSegment openSegment = cache.getOpenSegment();
    if (openSegment != null && openSegment.hasEntries()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("syncWithSnapshot : Found open segment {}, with end index {},"
                + " snapshotIndex {}", openSegment, openSegment.getEndIndex(),
            lastSnapshotIndex);
      }
      if (openSegment.getEndIndex() <= lastSnapshotIndex) {
        fileLogWorker.closeLogSegment(openSegment);
        cache.rollOpenSegment(false);
        cacheEviction.signal();
      }
    }
    return purgeImpl(lastSnapshotIndex).whenComplete((purged, e) -> updatePurgeIndex(purged));
  }

  @Override
  public void close() throws IOException {
    try(AutoCloseableLock writeLock = writeLock()) {
      LOG.info("Start closing {}", this);
      super.close();
      cacheEviction.close();
      cache.close();
    }
    fileLogWorker.close();
    storage.close();
    getRaftLogMetrics().unregister();
    LOG.info("Successfully closed {}", this);
  }

  SegmentedRaftLogCache getRaftLogCache() {
    return cache;
  }

  @Override
  public String toLogEntryString(LogEntryProto logEntry) {
    return LogProtoUtils.toLogEntryString(logEntry, stateMachine != null ?
        stateMachine::toStateMachineLogEntryString : null);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private RaftGroupMemberId memberId;
    private Division server;
    private StateMachine stateMachine;
    private Consumer<LogEntryProto> notifyTruncatedLogEntry;
    private BiFunction<LogEntryProto, Boolean, TransactionContext> getTransactionContext;
    private Runnable submitUpdateCommitEvent;
    private RaftStorage storage;
    private LongSupplier snapshotIndexSupplier = () -> RaftLog.INVALID_LOG_INDEX;
    private RaftProperties properties;

    private Builder() {}

    public Builder setMemberId(RaftGroupMemberId memberId) {
      this.memberId = memberId;
      return this;
    }

    public Builder setServer(Division server) {
      this.server = server;
      this.stateMachine = server.getStateMachine();
      return this;
    }

    public Builder setStateMachine(StateMachine stateMachine) {
      this.stateMachine = stateMachine;
      return this;
    }

    public Builder setNotifyTruncatedLogEntry(Consumer<LogEntryProto> notifyTruncatedLogEntry) {
      this.notifyTruncatedLogEntry = notifyTruncatedLogEntry;
      return this;
    }

    public Builder setGetTransactionContext(
        BiFunction<LogEntryProto, Boolean, TransactionContext> getTransactionContext) {
      this.getTransactionContext = getTransactionContext;
      return this;
    }

    public Builder setSubmitUpdateCommitEvent(Runnable submitUpdateCommitEvent) {
      this.submitUpdateCommitEvent = submitUpdateCommitEvent;
      return this;
    }

    public Builder setStorage(RaftStorage storage) {
      this.storage = storage;
      return this;
    }

    public Builder setSnapshotIndexSupplier(LongSupplier snapshotIndexSupplier) {
      this.snapshotIndexSupplier = snapshotIndexSupplier;
      return this;
    }

    public Builder setProperties(RaftProperties properties) {
      this.properties = properties;
      return this;
    }

    public SegmentedRaftLog build() {
      return new SegmentedRaftLog(this);
    }
  }
}
