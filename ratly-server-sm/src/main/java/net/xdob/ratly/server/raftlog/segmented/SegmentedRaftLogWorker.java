package net.xdob.ratly.server.raftlog.segmented;

import net.xdob.ratly.metrics.Timekeeper;
import net.xdob.ratly.server.Division;
import net.xdob.ratly.server.config.RaftServerConfigKeys;
import net.xdob.ratly.util.*;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.proto.raft.StateMachineLogEntryProto;
import net.xdob.ratly.protocol.ClientInvocationId;
import net.xdob.ratly.protocol.RaftGroupMemberId;
import net.xdob.ratly.protocol.exceptions.TimeoutIOException;
import net.xdob.ratly.server.metrics.SegmentedRaftLogMetrics;
import net.xdob.ratly.server.raftlog.LogProtoUtils;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.raftlog.RaftLogIOException;
import net.xdob.ratly.server.raftlog.RaftLogIndex;
import net.xdob.ratly.server.storage.RaftStorage;
import net.xdob.ratly.server.raftlog.segmented.SegmentedRaftLogCache.SegmentFileInfo;
import net.xdob.ratly.server.raftlog.segmented.SegmentedRaftLogCache.TruncationSegments;
import net.xdob.ratly.server.raftlog.segmented.SegmentedRaftLog.Task;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.statemachine.StateMachine;
import net.xdob.ratly.statemachine.DataStream;
import net.xdob.ratly.statemachine.TransactionContext;
import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * 该类负责处理 Raft 节点所有与 Raft 日志相关的 I/O 操作。
 */
class SegmentedRaftLogWorker {
  static final Logger LOG = LoggerFactory.getLogger(SegmentedRaftLogWorker.class);

  static final TimeDuration ONE_SECOND = TimeDuration.valueOf(1, TimeUnit.SECONDS);

  private static final String CLASS_NAME = JavaUtils.getClassSimpleName(SegmentedRaftLogWorker.class);
  static final String RUN_WORKER = CLASS_NAME + ".runWorker";

  static class StateMachineDataPolicy {
    private final boolean sync;
    private final TimeDuration syncTimeout;
    private final int syncTimeoutRetry;
    private final SegmentedRaftLogMetrics metrics;

    StateMachineDataPolicy(RaftProperties properties, SegmentedRaftLogMetrics metricRegistry) {
      this.sync = RaftServerConfigKeys.Log.StateMachineData.sync(properties);
      this.syncTimeout = RaftServerConfigKeys.Log.StateMachineData.syncTimeout(properties);
      this.syncTimeoutRetry = RaftServerConfigKeys.Log.StateMachineData.syncTimeoutRetry(properties);
      this.metrics = metricRegistry;
      Preconditions.assertTrue(syncTimeoutRetry >= -1);
    }

    boolean isSync() {
      return sync;
    }

    void getFromFuture(CompletableFuture<?> future, Supplier<Object> getName) throws IOException {
      Preconditions.assertTrue(isSync());
      TimeoutIOException lastException = null;
      for(int retry = 0; syncTimeoutRetry == -1 || retry <= syncTimeoutRetry; retry++) {
        try {
          IOUtils.getFromFuture(future, getName, syncTimeout);
          return;
        } catch(TimeoutIOException e) {
          LOG.warn("Timeout " + retry + (syncTimeoutRetry == -1? "/~": "/" + syncTimeoutRetry), e);
          lastException = e;
          metrics.onStateMachineDataWriteTimeout();
        }
      }
      Objects.requireNonNull(lastException, "lastException == null");
      throw lastException;
    }
  }

  static class WriteLogTasks {
    private final Queue<WriteLog> q = new LinkedList<>();
    private volatile long index;

    void offerOrCompleteFuture(WriteLog writeLog) {
      if (writeLog.getEndIndex() <= index || !offer(writeLog)) {
        writeLog.completeFuture();
      }
    }

    private synchronized boolean offer(WriteLog writeLog) {
      if (writeLog.getEndIndex() <= index) { // compare again synchronized
        return false;
      }
      q.offer(writeLog);
      return true;
    }

    synchronized void updateIndex(long i) {
      index = i;

      for(;;) {
        final Task peeked = q.peek();
        if (peeked == null || peeked.getEndIndex() > index) {
          return;
        }
        final Task polled = q.poll();
        Preconditions.assertTrue(polled == peeked);
        polled.completeFuture();
      }
    }
  }

  private final Consumer<Object> infoIndexChange = s -> LOG.info("{}: {}", this, s);
  private final Consumer<Object> traceIndexChange = s -> LOG.trace("{}: {}", this, s);

  private final String name;
  /**
   * rpc处理程序线程和io工作线程访问的任务队列。
   */
  private final DataBlockingQueue<Task> queue;
  private final WriteLogTasks writeTasks = new WriteLogTasks();
  private volatile boolean running = true;
  private final ExecutorService workerThreadExecutor;
  private final RaftStorage storage;
  @SuppressWarnings({"squid:S3077"}) // Suppress volatile for generic type
  private volatile SegmentedRaftLogOutputStream out;
  private final Runnable submitUpdateCommitEvent;
  private final StateMachine stateMachine;
  private final SegmentedRaftLogMetrics raftLogMetrics;
  private final ByteBuffer writeBuffer;

 /**
   * 已写入 SegmentedRaftLogOutputStream 但未刷新的条目数。
   */
  private int pendingFlushNum = 0;
  /** 已写入的最后一个条目的索引 */
  private long lastWrittenIndex;
  private volatile int flushBatchSize = 0;
  /** 已刷新的条目的最大索引 */
  private final RaftLogIndex flushIndex = new RaftLogIndex("flushIndex", 0);
  /** 可以逐出缓存的索引 - snapshotIndex 的最大值
   * 封闭段中的最大索引
   */
  private final RaftLogIndex safeCacheEvictIndex = new RaftLogIndex("safeCacheEvictIndex", 0);

  private final int forceSyncNum;

  private final long segmentMaxSize;
  private final long preallocatedSize;
  private final Division server;

  private final boolean asyncFlush;
  private final boolean unsafeFlush;
  private final ExecutorService flushExecutor;

  private final StateMachineDataPolicy stateMachineDataPolicy;

  SegmentedRaftLogWorker(RaftGroupMemberId memberId, StateMachine stateMachine, Runnable submitUpdateCommitEvent,
                         Division server, RaftStorage storage, RaftProperties properties,
                         SegmentedRaftLogMetrics metricRegistry) {
    this.name = memberId + "-" + JavaUtils.getClassSimpleName(getClass());
    LOG.info("new {} for {}", name, storage);

    this.submitUpdateCommitEvent = submitUpdateCommitEvent;
    this.stateMachine = stateMachine;
    this.raftLogMetrics = metricRegistry;
    this.storage = storage;
    this.server = server;
    final SizeInBytes queueByteLimit = RaftServerConfigKeys.Log.queueByteLimit(properties);
    final int queueElementLimit = RaftServerConfigKeys.Log.queueElementLimit(properties);
    this.queue =
        new DataBlockingQueue<>(name, queueByteLimit, queueElementLimit, Task::getSerializedSize);

    this.segmentMaxSize = RaftServerConfigKeys.Log.segmentSizeMax(properties).getSize();
    this.preallocatedSize = RaftServerConfigKeys.Log.preallocatedSize(properties).getSize();
    this.forceSyncNum = RaftServerConfigKeys.Log.forceSyncNum(properties);

    this.stateMachineDataPolicy = new StateMachineDataPolicy(properties, metricRegistry);

    this.workerThreadExecutor = Concurrents3.newSingleThreadExecutor(name);

    // 服务器 ID 在单元测试中可以为 null
    metricRegistry.addDataQueueSizeGauge(queue::getNumElements);
    metricRegistry.addLogWorkerQueueSizeGauge(writeTasks.q::size);
    metricRegistry.addFlushBatchSizeGauge(() -> flushBatchSize);

    final int bufferSize = RaftServerConfigKeys.Log.writeBufferSize(properties).getSizeInt();
    this.writeBuffer = ByteBuffer.allocateDirect(bufferSize);
    final int logEntryLimit = RaftServerConfigKeys.Log.Appender.bufferByteLimit(properties).getSizeInt();
    // 4 bytes (serialized size) + logEntryLimit + 4 bytes (checksum)
    if (bufferSize < logEntryLimit + 8) {
      throw new IllegalArgumentException(RaftServerConfigKeys.Log.WRITE_BUFFER_SIZE_KEY
          + " (= " + bufferSize
          + ") is less than " + RaftServerConfigKeys.Log.Appender.BUFFER_BYTE_LIMIT_KEY
          + " + 8 (= " + (logEntryLimit + 8) + ")");
    }
    this.unsafeFlush = RaftServerConfigKeys.Log.unsafeFlushEnabled(properties);
    this.asyncFlush = RaftServerConfigKeys.Log.asyncFlushEnabled(properties);
    if (asyncFlush && unsafeFlush) {
      throw new IllegalStateException("Cannot enable both " +  RaftServerConfigKeys.Log.UNSAFE_FLUSH_ENABLED_KEY +
          " and " + RaftServerConfigKeys.Log.ASYNC_FLUSH_ENABLED_KEY);
    }
    this.flushExecutor = (!asyncFlush && !unsafeFlush)? null
        : Concurrents3.newSingleThreadExecutor(name + "-flush");
  }

  void start(long latestIndex, long evictIndex, File openSegmentFile) throws IOException {
    LOG.trace("{} start(latestIndex={}, openSegmentFile={})", name, latestIndex, openSegmentFile);
    lastWrittenIndex = latestIndex;
    flushIndex.setUnconditionally(latestIndex, infoIndexChange);
    safeCacheEvictIndex.setUnconditionally(evictIndex, infoIndexChange);
    if (openSegmentFile != null) {
      Preconditions.assertTrue(openSegmentFile.exists());
      allocateSegmentedRaftLogOutputStream(openSegmentFile, true);
    }
    workerThreadExecutor.submit(this::run);
  }

  void close() {
    queue.close();
    this.running = false;
    Concurrents3.shutdownAndWait(TimeDuration.ONE_MINUTE, workerThreadExecutor,
        timeout -> LOG.warn("{}: shutdown timeout in {}", name, timeout));
    Optional.ofNullable(flushExecutor).ifPresent(ExecutorService::shutdown);
    IOUtils.cleanup(LOG, out);
    PlatformDependent.freeDirectBuffer(writeBuffer);
    LOG.info("{} close()", name);
  }

  /**
   * 刚刚在 follower 上安装了快照。需要更新 IO worker的相应状态。
   */
  void syncWithSnapshot(long lastSnapshotIndex) {
    queue.clear();
    lastWrittenIndex = lastSnapshotIndex;
    flushIndex.setUnconditionally(lastSnapshotIndex, infoIndexChange);
    safeCacheEvictIndex.setUnconditionally(lastSnapshotIndex, infoIndexChange);
    pendingFlushNum = 0;
  }

  @Override
  public String toString() {
    return name;
  }

  /**
   * This is protected by the RaftServer and RaftLog's lock.
   */
  private Task addIOTask(Task task) {
    LOG.debug("{} adds IO task {}", name, task);
    try(UncheckedAutoCloseable ignored = raftLogMetrics.startQueuingDelayTimer()) {
      for(; !queue.offer(task, ONE_SECOND); ) {
        Preconditions.assertTrue(isAlive(),
            "the worker thread is not alive");
      }
    } catch (Exception e) {
      if (e instanceof InterruptedException && !running) {
        LOG.info("Got InterruptedException when adding task " + task
            + ". The SegmentedRaftLogWorker already stopped.");
        Thread.currentThread().interrupt();
      } else {
        LOG.error("Failed to add IO task {}", task, e);
        Optional.ofNullable(server).ifPresent(Division::close);
      }
      task.discard();
    }
    task.startTimerOnEnqueue(raftLogMetrics.getEnqueuedTimer());
    return task;
  }

  boolean isAlive() {
    return running && !workerThreadExecutor.isTerminated();
  }

  private void run() {
    // if and when a log task encounters an exception
    RaftLogIOException logIOException = null;

    CodeInjectionForTesting.execute(RUN_WORKER, server == null ? null : server.getId(), null, queue);
    while (running) {
      try {
        Task task = queue.poll(ONE_SECOND);
        if (task != null) {
          task.stopTimerOnDequeue();
          try {
            if (logIOException != null) {
              throw logIOException;
            } else {
              try (UncheckedAutoCloseable ignored = raftLogMetrics.startTaskExecutionTimer(task.getClass())) {
                task.execute();
              }
            }
          } catch (IOException e) {
            if (task.getEndIndex() < lastWrittenIndex) {
              LOG.info("Ignore IOException when handling task " + task
                  + " which is smaller than the lastWrittenIndex."
                  + " There should be a snapshot installed.", e);
            } else {
              task.failed(e);
              if (logIOException == null) {
                logIOException = new RaftLogIOException("Log already failed"
                    + " at index " + task.getEndIndex()
                    + " for task " + task, e);
              }
              continue;
            }
          }
          task.done();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        if (running) {
          LOG.warn("{} got interrupted while still running",
              Thread.currentThread().getName());
        }
        LOG.info(Thread.currentThread().getName()
            + " was interrupted, exiting. There are " + queue.getNumElements()
            + " tasks remaining in the queue.");
        break;
      } catch (Exception e) {
        if (!running) {
          LOG.info("{} got closed and hit exception",
              Thread.currentThread().getName(), e);
        } else {
          LOG.error("{} hit exception", Thread.currentThread().getName(), e);
          Optional.ofNullable(server).ifPresent(Division::close);
        }
      }
    }

    queue.clear(Task::discard);
  }

  private boolean shouldFlush() {
    if (out == null) {
      return false;
    } else if (pendingFlushNum >= forceSyncNum) {
      return true;
    }
    return pendingFlushNum > 0 && !(queue.peek() instanceof WriteLog);
  }

  private void flushIfNecessary() throws IOException {
    if (shouldFlush()) {
      raftLogMetrics.onRaftLogFlush();
      LOG.debug("{}: flush {}", name, out);
      try(UncheckedAutoCloseable ignored = raftLogMetrics.startFlushTimer()) {
        final CompletableFuture<Void> f = stateMachine != null ?
            stateMachine.data().flush(lastWrittenIndex) :
            CompletableFuture.completedFuture(null);
        if (stateMachineDataPolicy.isSync()) {
          stateMachineDataPolicy.getFromFuture(f, () -> this + "-flushStateMachineData");
        }
        flushBatchSize = (int)(lastWrittenIndex - flushIndex.get());
        if (unsafeFlush) {
          // unsafe-flush: call updateFlushedIndexIncreasingly() without waiting the underlying FileChannel.force(..).
          unsafeFlushOutStream();
          updateFlushedIndexIncreasingly();
        } else if (asyncFlush) {
          asyncFlushOutStream(f);
        } else {
          flushOutStream();
          if (!stateMachineDataPolicy.isSync()) {
            IOUtils.getFromFuture(f, () -> this + "-flushStateMachineData");
          }
          updateFlushedIndexIncreasingly();
        }
      }
    }
  }

  private void unsafeFlushOutStream() throws IOException {
    final Timekeeper.Context logSyncTimerContext = raftLogMetrics.getSyncTimer().time();
    out.asyncFlush(flushExecutor).whenComplete((v, e) -> logSyncTimerContext.stop());
  }

  private void asyncFlushOutStream(CompletableFuture<Void> stateMachineFlush) throws IOException {
    final Timekeeper.Context logSyncTimerContext = raftLogMetrics.getSyncTimer().time();
    out.asyncFlush(flushExecutor)
        .thenCombine(stateMachineFlush, (async, sm) -> async)
        .whenComplete((v, e) -> {
          updateFlushedIndexIncreasingly(lastWrittenIndex);
          logSyncTimerContext.stop();
        });
  }

  private void flushOutStream() throws IOException {
    try(UncheckedAutoCloseable ignored = Timekeeper.start(raftLogMetrics.getSyncTimer())) {
      out.flush();
    }
  }

  private void updateFlushedIndexIncreasingly() {
    updateFlushedIndexIncreasingly(lastWrittenIndex);
  }

  private void updateFlushedIndexIncreasingly(long index) {
    flushIndex.updateIncreasingly(index, traceIndexChange);
    postUpdateFlushedIndex(Math.toIntExact(lastWrittenIndex - index));
    writeTasks.updateIndex(index);
  }

  private void postUpdateFlushedIndex(int count) {
    pendingFlushNum = count;
    Optional.ofNullable(submitUpdateCommitEvent).ifPresent(Runnable::run);
  }

  /**
   * The following several methods (startLogSegment, rollLogSegment,
   * writeLogEntry, and truncate) are only called by SegmentedRaftLog which is
   * protected by RaftServer's lock.
   * Thus all the tasks are created and added sequentially.
   */
  void startLogSegment(long startIndex) {
    LOG.info("{}: Starting segment from index:{}", name, startIndex);
    addIOTask(new StartLogSegment(startIndex));
  }

  void rollLogSegment(LogSegment segmentToClose) {
    LOG.info("{}: Rolling segment {} to index:{}", name,
        segmentToClose.toString(), segmentToClose.getEndIndex());
    addIOTask(new FinalizeLogSegment(segmentToClose));
    addIOTask(new StartLogSegment(segmentToClose.getEndIndex() + 1));
  }

  Task writeLogEntry(ReferenceCountedObject<LogEntryProto> entry,
                     LogEntryProto removedStateMachineData, TransactionContext context) {
    return addIOTask(new WriteLog(entry, removedStateMachineData, context));
  }

  Task truncate(TruncationSegments ts, long index) {
    LOG.info("{}: Truncating segments {}, start index {}", name, ts, index);
    return addIOTask(new TruncateLog(ts, index));
  }

  void closeLogSegment(LogSegment segmentToClose) {
    LOG.info("{}: Closing segment {} to index: {}", name,
        segmentToClose.toString(), segmentToClose.getEndIndex());
    addIOTask(new FinalizeLogSegment(segmentToClose));
  }

  Task purge(TruncationSegments ts) {
    return addIOTask(new PurgeLog(ts));
  }

  private final class PurgeLog extends Task {
    private final TruncationSegments segments;

    private PurgeLog(TruncationSegments segments) {
      this.segments = segments;
    }

    @Override
    void execute() throws IOException {
      if (segments.getToDelete() != null) {
        try(UncheckedAutoCloseable ignored = raftLogMetrics.startPurgeTimer()) {
          for (SegmentFileInfo fileInfo : segments.getToDelete()) {
            final Path deleted = FileUtils.deleteFile(fileInfo.getFile(storage));
            LOG.info("{}: Purged RaftLog segment: info={}, path={}", name, fileInfo, deleted);
          }
        }
      }
    }

    @Override
    long getEndIndex() {
      return segments.maxEndIndex();
    }
  }

  private class WriteLog extends Task {
    private final LogEntryProto entry;
    private final CompletableFuture<?> stateMachineFuture;
    private final CompletableFuture<Long> combined;
    private final AtomicReference<ReferenceCountedObject<LogEntryProto>> ref = new AtomicReference<>();

    WriteLog(ReferenceCountedObject<LogEntryProto> entryRef, LogEntryProto removedStateMachineData,
        TransactionContext context) {
      LogEntryProto origEntry = entryRef.get();
      this.entry = removedStateMachineData;
      if (this.entry == origEntry) {
        final StateMachineLogEntryProto proto = origEntry.hasStateMachineLogEntry() ?
            origEntry.getStateMachineLogEntry(): null;
        if (stateMachine != null && proto != null && proto.getType() == StateMachineLogEntryProto.Type.DATASTREAM) {
          final ClientInvocationId invocationId = ClientInvocationId.valueOf(proto);
          final CompletableFuture<DataStream> removed = server.getDataStreamMap().remove(invocationId);
          this.stateMachineFuture = removed == null? stateMachine.data().link(null, origEntry)
              : removed.thenApply(stream -> stateMachine.data().link(stream, origEntry));
        } else {
          this.stateMachineFuture = null;
        }
        entryRef.retain();
        this.ref.set(entryRef);
      } else {
        try {
          // this.entry != origEntry if it has state machine data
          this.stateMachineFuture = stateMachine.data().write(entryRef, context);
        } catch (Exception e) {
          LOG.error(name + ": writeStateMachineData failed for index " + origEntry.getIndex()
              + ", entry=" + LogProtoUtils.toLogEntryString(origEntry, stateMachine::toStateMachineLogEntryString), e);
          throw e;
        }
      }
      this.combined = stateMachineFuture == null? super.getFuture()
          : super.getFuture().thenCombine(stateMachineFuture, (index, stateMachineResult) -> index);
    }

    @Override
    void failed(IOException e) {
      stateMachine.event().notifyLogFailed(e, entry);
      super.failed(e);
      discard();
      LOG.warn("{}: failed to write log entry ", name, e);
    }

    @Override
    int getSerializedSize() {
      return LogProtoUtils.getSerializedSize(entry);
    }

    @Override
    CompletableFuture<Long> getFuture() {
      return combined;
    }

    @Override
    void done() {
      writeTasks.offerOrCompleteFuture(this);
      discard();
    }

    @Override
    void discard() {
      final ReferenceCountedObject<LogEntryProto> entryRef = ref.getAndSet(null);
      if (entryRef != null) {
        entryRef.release();
      }
    }

    @Override
    public void execute() throws IOException {
      if (stateMachineDataPolicy.isSync() && stateMachineFuture != null) {
        stateMachineDataPolicy.getFromFuture(stateMachineFuture, () -> this + "-writeStateMachineData");
      }

      raftLogMetrics.onRaftLogAppendEntry();
      Preconditions.assertTrue(out != null);
      Preconditions.assertTrue(lastWrittenIndex + 1 == entry.getIndex(),
          "lastWrittenIndex == %s, entry == %s", lastWrittenIndex, entry);
      out.write(entry);
      lastWrittenIndex = entry.getIndex();
      pendingFlushNum++;
      flushIfNecessary();
    }

    @Override
    long getEndIndex() {
      return entry.getIndex();
    }

    @Override
    public String toString() {
      return super.toString() + ": " + LogProtoUtils.toLogEntryString(
          entry, stateMachine == null? null: stateMachine::toStateMachineLogEntryString);
    }
  }

  private File getFile(LogSegmentStartEnd startEnd) {
    return startEnd.getFile(storage);
  }

  private class FinalizeLogSegment extends Task {
    private final long startIndex;
    private final long endIndex;

    FinalizeLogSegment(LogSegment segmentToClose) {
      Preconditions.assertTrue(segmentToClose != null, "Log segment to be rolled is null");
      this.startIndex = segmentToClose.getStartIndex();
      this.endIndex = segmentToClose.getEndIndex();
    }

    @Override
    public void execute() throws IOException {
      freeSegmentedRaftLogOutputStream();

      final LogSegmentStartEnd openStartEnd = LogSegmentStartEnd.valueOf(startIndex);
      final File openFile = getFile(openStartEnd);
      Preconditions.assertTrue(openFile.exists(),
          () -> name + ": File " + openFile + " to be rolled does not exist");
      if (endIndex - startIndex + 1 > 0) {
        // finalize the current open segment
        final File dstFile = getFile(LogSegmentStartEnd.valueOf(startIndex, endIndex));
        Preconditions.assertTrue(!dstFile.exists());

        FileUtils.move(openFile, dstFile);
        LOG.info("{}: Rolled log segment from {} to {}", name, openFile, dstFile);
      } else { // delete the file of the empty segment
        final Path deleted = FileUtils.deleteFile(openFile);
        LOG.info("{}: Deleted empty RaftLog segment: startEnd={}, path={}", name, openStartEnd, deleted);
      }
      updateFlushedIndexIncreasingly();
      safeCacheEvictIndex.updateToMax(endIndex, traceIndexChange);
    }

    @Override
    void failed(IOException e) {
      // 不是特定日志条目的失败，而是整个段的失败
      stateMachine.event().notifyLogFailed(e, null);
      super.failed(e);
    }

    @Override
    long getEndIndex() {
      return endIndex;
    }

    @Override
    public String toString() {
      return super.toString() + ": " + "startIndex=" + startIndex + " endIndex=" + endIndex;
    }
  }

  private class StartLogSegment extends Task {
    private final long newStartIndex;

    StartLogSegment(long newStartIndex) {
      this.newStartIndex = newStartIndex;
    }

    @Override
    void execute() throws IOException {
      final File openFile = getFile(LogSegmentStartEnd.valueOf(newStartIndex));
      Preconditions.assertTrue(!openFile.exists(), "open file %s exists for %s",
          openFile, name);
      Preconditions.assertTrue(pendingFlushNum == 0);
      allocateSegmentedRaftLogOutputStream(openFile, false);
      Preconditions.assertTrue(openFile.exists(), "Failed to create file %s for %s",
          openFile.getAbsolutePath(), name);
      LOG.info("{}: created new log segment {}", name, openFile);
    }

    @Override
    long getEndIndex() {
      return newStartIndex;
    }
  }

  private class TruncateLog extends Task {
    private final TruncationSegments segments;
    private CompletableFuture<Void> stateMachineFuture = null;

    TruncateLog(TruncationSegments ts, long index) {
      this.segments = ts;
      if (stateMachine != null) {
        // 在获取 RaftLog 写锁时创建 TruncateLog 和 WriteLog 实例。
        // 状态机调用是在构造函数内部进行的，以确保它受锁保护。
        // 这是为了确保状态机能够确定需要截断的索引，因为状态机的调用将按日志操作的顺序进行。
        stateMachineFuture = stateMachine.data().truncate(index);
      }
    }

    @Override
    void execute() throws IOException {
      freeSegmentedRaftLogOutputStream();

      if (segments.getToDelete() != null && segments.getToDelete().length > 0) {
        long minStart = segments.getToDelete()[0].getStartIndex();
        for (SegmentFileInfo del : segments.getToDelete()) {
          final File delFile = del.getFile(storage);
          Preconditions.assertTrue(delFile.exists(),
              "File %s to be deleted does not exist", delFile);
          final Path deleted = FileUtils.deleteFile(delFile);
          LOG.info("{}: Deleted RaftLog segment for {}: path={}", name, segments.getReason(), deleted);
          minStart = Math.min(minStart, del.getStartIndex());
        }
        if (segments.getToTruncate() == null) {
          lastWrittenIndex = minStart - 1;
        }
      }

      if (segments.getToTruncate() != null) {
        final File fileToTruncate = segments.getToTruncate().getFile(storage);
        Preconditions.assertTrue(fileToTruncate.exists(),
            "File %s to be truncated does not exist", fileToTruncate);
        FileUtils.truncateFile(fileToTruncate, segments.getToTruncate().getTargetLength());

        // rename the file
        final File dstFile = segments.getToTruncate().getNewFile(storage);
        Preconditions.assertTrue(!dstFile.exists(),
            "Truncated file %s already exists ", dstFile);
        FileUtils.move(fileToTruncate, dstFile);
        LOG.info("{}: Truncated log file {} to length {} and moved it to {}", name,
            fileToTruncate, segments.getToTruncate().getTargetLength(), dstFile);

        // 更新 lastWrittenIndex
        lastWrittenIndex = segments.getToTruncate().getNewEndIndex();
      }

      if (stateMachineFuture != null) {
        IOUtils.getFromFuture(stateMachineFuture, () -> this + "-truncateStateMachineData");
      }
      flushIndex.setUnconditionally(lastWrittenIndex, infoIndexChange);
      safeCacheEvictIndex.setUnconditionally(lastWrittenIndex, infoIndexChange);
      postUpdateFlushedIndex(0);
    }

    @Override
    long getEndIndex() {
      if (segments.getToTruncate() != null) {
        return segments.getToTruncate().getNewEndIndex();
      } else if (segments.getToDelete().length > 0) {
        return segments.getToDelete()[segments.getToDelete().length - 1].getEndIndex();
      }
      return RaftLog.INVALID_LOG_INDEX;
    }

    @Override
    public String toString() {
      return super.toString() + ": " + segments;
    }
  }

  long getFlushIndex() {
    return flushIndex.get();
  }

  long getSafeCacheEvictIndex() {
    return safeCacheEvictIndex.get();
  }

  private void freeSegmentedRaftLogOutputStream() {
    IOUtils.cleanup(LOG, out);
    out = null;
    Preconditions.assertTrue(writeBuffer.position() == 0);
  }

  private void allocateSegmentedRaftLogOutputStream(File file, boolean append) throws IOException {
    Preconditions.assertNull(out, "out");
    Preconditions.assertSame(0, writeBuffer.position(), "writeBuffer.position()");
    out = new SegmentedRaftLogOutputStream(file, append, segmentMaxSize,
        preallocatedSize, writeBuffer);
  }
}
