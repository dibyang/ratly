package net.xdob.ratly.server.raftlog.memory;

import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.protocol.RaftGroupMemberId;
import net.xdob.ratly.server.metrics.RaftLogMetricsBase;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.server.raftlog.LogProtoUtils;
import net.xdob.ratly.server.raftlog.RaftLogBase;
import net.xdob.ratly.server.raftlog.LogEntryHeader;
import net.xdob.ratly.server.raftlog.RaftLogIOException;
import net.xdob.ratly.server.storage.RaftStorageMetadata;
import net.xdob.ratly.statemachine.TransactionContext;
import net.xdob.ratly.util.AutoCloseableLock;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.ReferenceCountedObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * A simple RaftLog implementation in memory. Used only for testing.
 */
public class MemoryRaftLog extends RaftLogBase {
  /**
   * 内存中的Raft日志条目列表。
   */
  static class EntryList {
    private final List<LogEntryProto> entries = new ArrayList<>();

    /**
     * 获取指定索引的日志条目。
     *
     * @param i 日志条目的索引
     * @return 指定索引的日志条目，如果索引无效则返回null
     */
    LogEntryProto get(int i) {
      return i >= 0 && i < entries.size() ? entries.get(i) : null;
    }

    TermIndex getTermIndex(int i) {
      return TermIndex.valueOf(get(i));
    }

    private LogEntryHeader getLogEntryHeader(int i) {
      return LogEntryHeader.valueOf(get(i));
    }

    int size() {
      return entries.size();
    }

    /**
     * 截断日志条目列表到指定索引。
     *
     * @param index 截断的索引
     */
    void truncate(int index) {
      if (entries.size() > index) {
        clear(index, entries.size());
      }
    }

    void purge(int index) {
      if (entries.size() > index) {
        clear(0, index);
      }
    }

    void clear(int from, int to) {
      entries.subList(from, to).clear();
    }

    /**
     * 添加一个新的日志条目到列表中。
     *
     * @param entryRef 要添加的日志条目
     */
    void add(LogEntryProto entryRef) {
      entries.add(entryRef);
    }
  }

  private final EntryList entries = new EntryList();
  private final AtomicReference<RaftStorageMetadata> metadata = new AtomicReference<>(RaftStorageMetadata.getDefault());
  private final RaftLogMetricsBase metrics;

  public MemoryRaftLog(RaftGroupMemberId memberId,
                       LongSupplier commitIndexSupplier,
                       RaftProperties properties) {
    super(memberId, commitIndexSupplier, properties);
    this.metrics = new RaftLogMetricsBase(memberId);
  }

  @Override
  public void close() throws IOException {
    super.close();
    metrics.unregister();
  }

  @Override
  public RaftLogMetricsBase getRaftLogMetrics() {
    return metrics;
  }

  /**
   * 获取指定索引的日志条目。
   *
   * @param index 日志条目的索引
   * @return 指定索引的日志条目
   * @throws RaftLogIOException 如果获取日志条目时发生IO异常
   */
  @Override
  public LogEntryProto get(long index) throws RaftLogIOException {
    final ReferenceCountedObject<LogEntryProto> ref = retainLog(index);
    try {
      return LogProtoUtils.copy(ref.get());
    } finally {
      ref.release();
    }
  }

  @Override
  public ReferenceCountedObject<LogEntryProto> retainLog(long index) {
    checkLogState();
    try (AutoCloseableLock readLock = readLock()) {
      final LogEntryProto entry = entries.get(Math.toIntExact(index));
      final ReferenceCountedObject<LogEntryProto> ref = ReferenceCountedObject.wrap(entry);
      ref.retain();
      return ref;
    }
  }

  @Override
  public EntryWithData getEntryWithData(long index) throws RaftLogIOException {
    throw new UnsupportedOperationException("Use retainEntryWithData(" + index + ") instead.");
  }

  @Override
  public ReferenceCountedObject<EntryWithData> retainEntryWithData(long index) {
    final ReferenceCountedObject<LogEntryProto> ref = retainLog(index);
    return newEntryWithData(ref);
  }

  /**
   * 获取指定索引的日志条目的TermIndex。
   *
   * @param index 日志条目的索引
   * @return 指定索引的日志条目的TermIndex
   */
  @Override
  public TermIndex getTermIndex(long index) {
    checkLogState();
    try(AutoCloseableLock readLock = readLock()) {
      return entries.getTermIndex(Math.toIntExact(index));
    }
  }

  @Override
  public LogEntryHeader[] getEntries(long startIndex, long endIndex) {
    checkLogState();
    try(AutoCloseableLock readLock = readLock()) {
      if (startIndex >= entries.size()) {
        return null;
      }
      final int from = Math.toIntExact(startIndex);
      final int to = Math.toIntExact(Math.min(entries.size(), endIndex));
      final LogEntryHeader[] headers = new LogEntryHeader[to - from];
      for (int i = 0; i < headers.length; i++) {
        headers[i] = entries.getLogEntryHeader(i);
      }
      return headers;
    }
  }

  /**
   * 截断日志到指定索引。
   *
   * @param index 截断的索引
   * @return 完成后的Future，包含截断的索引
   */
  @Override
  protected CompletableFuture<Long> truncateImpl(long index) {
    checkLogState();
    try(AutoCloseableLock writeLock = writeLock()) {
      Preconditions.assertTrue(index >= 0);
      entries.truncate(Math.toIntExact(index));
    }
    return CompletableFuture.completedFuture(index);
  }


  @Override
  protected CompletableFuture<Long> purgeImpl(long index) {
    try (AutoCloseableLock writeLock = writeLock()) {
      Preconditions.assertTrue(index >= 0);
      entries.purge(Math.toIntExact(index));
    }
    return CompletableFuture.completedFuture(index);
  }

  @Override
  public TermIndex getLastEntryTermIndex() {
    checkLogState();
    try(AutoCloseableLock readLock = readLock()) {
      return entries.getTermIndex(entries.size() - 1);
    }
  }

  /**
   * 追加一个新的日志条目。
   *
   * @param entryRef 要追加的日志条目
   * @param context 事务上下文
   * @return 完成后的Future，包含追加的日志条目的索引
   */
  @Override
  protected CompletableFuture<Long> appendEntryImpl(ReferenceCountedObject<LogEntryProto> entryRef,
      TransactionContext context) {
    checkLogState();
    LogEntryProto entry = entryRef.retain();
    try (AutoCloseableLock writeLock = writeLock()) {
      validateLogEntry(entry);
      entries.add(entry);
    } finally {
      entryRef.release();
    }
    return CompletableFuture.completedFuture(entry.getIndex());
  }

  @Override
  public long getStartIndex() {
    return entries.size() == 0? INVALID_LOG_INDEX: entries.getTermIndex(0).getIndex();
  }

  @Override
  public List<CompletableFuture<Long>> appendImpl(ReferenceCountedObject<List<LogEntryProto>> entriesRef) {
    checkLogState();
    final List<LogEntryProto> logEntryProtos = entriesRef.retain();
    if (logEntryProtos == null || logEntryProtos.isEmpty()) {
      entriesRef.release();
      return Collections.emptyList();
    }
    try (AutoCloseableLock writeLock = writeLock()) {
      // 在截断条目之前，我们需要先检查是否有重复的条目。
      // 如果领导者发送了条目6，然后发送了条目7，接着再次发送了条目6，
      // 没有这个检查，跟随者可能会在接收到条目6时截断条目7。
      // 在领导者检测到这个截断之前，领导者可能会认为条目7已经被提交，
      // 但实际上该条目尚未被提交到大多数对等体的磁盘上。
      boolean toTruncate = false;
      int truncateIndex = (int) logEntryProtos.get(0).getIndex();
      int index = 0;
      for (; truncateIndex < getNextIndex() && index < logEntryProtos.size();
           index++, truncateIndex++) {
        if (this.entries.get(truncateIndex).getTerm() !=
            logEntryProtos.get(index).getTerm()) {
          toTruncate = true;
          break;
        }
      }
      final List<CompletableFuture<Long>> futures;
      if (toTruncate) {
        futures = new ArrayList<>(logEntryProtos.size() - index + 1);
        futures.add(truncate(truncateIndex));
      } else {
        futures = new ArrayList<>(logEntryProtos.size() - index);
      }
      for (int i = index; i < logEntryProtos.size(); i++) {
        LogEntryProto logEntryProto = logEntryProtos.get(i);
        entries.add(LogProtoUtils.copy(logEntryProto));
        futures.add(CompletableFuture.completedFuture(logEntryProto.getIndex()));
      }
      return futures;
    } finally {
      entriesRef.release();
    }
  }

  public String getEntryString() {
    return "entries=" + entries;
  }

  @Override
  public long getFlushIndex() {
    return getNextIndex() - 1;
  }

  @Override
  public void persistMetadata(RaftStorageMetadata newMetadata) {
    metadata.set(newMetadata);
  }

  @Override
  public RaftStorageMetadata loadMetadata() {
    return metadata.get();
  }

  @Override
  public CompletableFuture<Long> onSnapshotInstalled(long lastSnapshotIndex) {
    return CompletableFuture.completedFuture(lastSnapshotIndex);
    // 不执行任何操作
  }
}
