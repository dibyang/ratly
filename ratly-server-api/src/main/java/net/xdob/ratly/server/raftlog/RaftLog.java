
package net.xdob.ratly.server.raftlog;

import net.xdob.ratly.proto.raft.*;
import net.xdob.ratly.server.metrics.RaftLogMetrics;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.storage.RaftStorageMetadata;
import net.xdob.ratly.util.ReferenceCountedObject;
import net.xdob.ratly.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Raft 服务的事务日志核心功能。它扩展了 RaftLogSequentialOps（用于顺序操作）和 Closeable（支持资源关闭）。
 *
 */
public interface RaftLog extends RaftLogSequentialOps, Closeable {
  Logger LOG = LoggerFactory.getLogger(RaftLog.class);

  /** The least valid log index, i.e. the index used when writing to an empty log. */
  long LEAST_VALID_LOG_INDEX = 0L;
  /** Invalid log index is used to indicate that the log index is missing. */
  long INVALID_LOG_INDEX = LEAST_VALID_LOG_INDEX - 1;

  /**
   * 检查日志中是否包含指定的 TermIndex。
   */
  default boolean contains(TermIndex ti) {
    Objects.requireNonNull(ti, "ti == null");
    return ti.equals(getTermIndex(ti.getIndex()));
  }

  /**
   * 获取指定日志索引对应的 TermIndex，如果不存在返回 null。
   */
  TermIndex getTermIndex(long index);

  /**
   * 直接返回指定索引的日志条目副本。
   * @return null if the log entry is not found in this log;
   *         otherwise, return a copy of the log entry corresponding to the given index.
   * @deprecated use {@link RaftLog#retainLog(long)} instead in order to avoid copying.
   */
  @Deprecated
  LogEntryProto get(long index) throws RaftLogIOException;

  /**
   * 返回指定日志条目的引用计数对象，调用方需在使用完毕后手动释放引用。
   * @return a retained {@link ReferenceCountedObject} to the log entry corresponding to the given index if it exists;
   *         otherwise, return null.
   *         Since the returned reference is retained, the caller must call {@link ReferenceCountedObject#release()}}
   *         after use.
   */
  default ReferenceCountedObject<LogEntryProto> retainLog(long index) throws RaftLogIOException {
    ReferenceCountedObject<LogEntryProto> wrap = ReferenceCountedObject.wrap(get(index));
    wrap.retain();
    return wrap;
  }

  /**
   * 返回包含状态机数据的日志条目。
   * @return null if the log entry is not found in this log;
   *         otherwise, return the {@link EntryWithData} corresponding to the given index.
   * @deprecated use {@link #retainEntryWithData(long)}.
   */
  @Deprecated
  EntryWithData getEntryWithData(long index) throws RaftLogIOException;

  /**
   * 返回包含状态机数据的日志条目的引用计数对象。
   * @return null if the log entry is not found in this log;
   *         otherwise, return a retained reference of the {@link EntryWithData} corresponding to the given index.
   *         Since the returned reference is retained, the caller must call {@link ReferenceCountedObject#release()}}
   *         after use.
   */
  default ReferenceCountedObject<EntryWithData> retainEntryWithData(long index) throws RaftLogIOException {
    final ReferenceCountedObject<EntryWithData> wrap = ReferenceCountedObject.wrap(getEntryWithData(index));
    wrap.retain();
    return wrap;
}

  /**
   * 获取指定范围内的日志条目头信息（不包含具体数据）。
   * @param startIndex the starting log index (inclusive)
   * @param endIndex the ending log index (exclusive)
   * @return null if entries are unavailable in this log;
   *         otherwise, return the log entry headers within the given index range.
   */
  LogEntryHeader[] getEntries(long startIndex, long endIndex);

  /** 获取日志的起始索引。
   * @return the index of the starting entry of this log.
   */
  long getStartIndex();

  /**
   * 获取下一个可追加日志的索引。
   * @return the index of the next entry to append.
   */
  default long getNextIndex() {
    final TermIndex last = getLastEntryTermIndex();
    if (last == null) {
      // if the log is empty, the last committed index should be consistent with
      // the last index included in the latest snapshot.
      return getLastCommittedIndex() + 1;
    }
    return last.getIndex() + 1;
  }

  /**
   * 获取最后提交的日志索引。
   * @return the index of the last entry that has been committed.
   */
  long getLastCommittedIndex();

  /**
   * 获取最新快照的索引。
   * @return the index of the latest snapshot.
   */
  long getSnapshotIndex();

  /**
   * 获取已刷入本地存储的最后日志索引。
   * @return the index of the last entry that has been flushed to the local storage.
   */
  long getFlushIndex();

  /**
   * 获取最后一条日志条目的 TermIndex。
   * @return the {@link TermIndex} of the last log entry.
   */
  TermIndex getLastEntryTermIndex();

  /**
   * 返回日志相关的性能指标。
   * @return the {@link RaftLogMetrics}.
   */
  RaftLogMetrics getRaftLogMetrics();

  /**
   * 根据多数派索引更新提交索引。
   * Update the commit index.
   * @param majorityIndex the index that has achieved majority.
   * @param currentTerm the current term.
   * @param isLeader Is this server the leader?
   * @return true if commit index is changed; otherwise, return false.
   */
  boolean updateCommitIndex(long majorityIndex, long currentTerm, boolean isLeader);

  /**
   * 更新快照索引，同时可能影响提交索引。
   * Update the snapshot index with the given index.
   * Note that the commit index may also be changed by this update.
   */
  void updateSnapshotIndex(long newSnapshotIndex);

  /**
   * 打开日志以便读写，同时初始化日志状态。
   * Open this log for read and write.
   */
  void open(long lastIndexInSnapshot, Consumer<LogEntryProto> consumer) throws IOException;

  /**
   * 异步清理建议的索引及之前的日志。
   * Purge asynchronously the log transactions.
   * The implementation may choose to purge an index other than the suggested index.
   *
   * @param suggestedIndex the suggested index (inclusive) to be purged.
   * @return the future of the actual purged log index.
   */
  CompletableFuture<Long> purge(long suggestedIndex);

  /**
   * 在安装快照后更新日志状态，并清理过期日志。
   * A snapshot is installed so that the indices and other information of this log must be updated.
   * This log may also purge the outdated entries.
   *
   * @return the future of the actual purged log index (inclusive).
   */
  CompletableFuture<Long> onSnapshotInstalled(long lastSnapshotIndex);

  /**
   * 持久化存储元数据。
   * Persist the given metadata.
   */
  void persistMetadata(RaftStorageMetadata metadata) throws IOException;

  /**
   * 加载日志存储的元数据。
   * Load metadata.
   */
  RaftStorageMetadata loadMetadata() throws IOException;

  /**
   * 表示一个日志条目与状态机数据的组合
   * 当日志条目包含状态机相关数据时，该接口用于重建和访问完整数据。
   * Log entry with state machine data.
   *
   * When both {@link LogEntryProto#hasStateMachineLogEntry()} and
   * {@link StateMachineLogEntryProto#hasStateMachineEntry()} are true,
   * the {@link StateMachineEntryProto} is removed from the original {@link LogEntryProto}
   * before appending to this log.
   * The {@link StateMachineEntryProto} is stored by the state machine but not in this log.
   * When reading the log entry, this class rebuilds the original {@link LogEntryProto}
   * containing both the log entry and the state machine data.
   */
  interface EntryWithData {
    /**
     * 返回日志条目的索引。
     * @return the index of this entry.
     */
    default long getIndex() {
      try {
        return getEntry(TimeDuration.ONE_MINUTE).getIndex();
      } catch (Exception e) {
        throw new IllegalStateException("Failed to getIndex", e);
      }
    }

    /**
     * 返回日志条目的序列化大小。
     * @return the serialized size including both log entry and state machine data.
     */
    int getSerializedSize();

    /**
     * 返回包含日志和状态机数据的完整 LogEntryProto，支持超时。
     * @return the {@link LogEntryProto} containing both the log entry and the state machine data.
     */
    LogEntryProto getEntry(TimeDuration timeout) throws RaftLogIOException, TimeoutException;
  }
}
