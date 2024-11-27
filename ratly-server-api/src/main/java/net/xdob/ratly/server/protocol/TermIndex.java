package net.xdob.ratly.server.protocol;

import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.proto.raft.TermIndexProto;
import net.xdob.ratly.server.raftlog.RaftLog;

import java.util.Comparator;
import java.util.Optional;

/**
 * TermIndex 是 Raft 协议中的一个核心概念，结合任期号（term）和日志索引（index），用于唯一标识某一日志条目。
 * 它在 Raft 的日志复制和一致性保证中扮演重要角色，比如用于比较日志的新旧程度和定位特定日志条目。
 */
public interface TermIndex extends Comparable<TermIndex> {
  /**
   * The initial value.
   * When a new Raft group starts,
   * all the servers has term 0 and index -1 (= {@link RaftLog#INVALID_LOG_INDEX}).
   * Note that term is incremented during leader election
   * and index is incremented when writing to the {@link RaftLog}.
   * The least term and index possibly written to the {@link RaftLog}
   * are respectively 1 and 0 (= {@link RaftLog#LEAST_VALID_LOG_INDEX}).
   */
  TermIndex INITIAL_VALUE = valueOf(0, RaftLog.INVALID_LOG_INDEX);

  /** An empty {@link TermIndex} array. */
  TermIndex[] EMPTY_ARRAY = {};

  /** 返回当前 TermIndex 对象的任期号。*/
  long getTerm();

  /** 返回当前 TermIndex 对象的日志索引。*/
  long getIndex();

  /**
   * 将当前对象转换为 Protobuf 格式，用于网络通信和持久化。
   * @return the {@link TermIndexProto}.
   */
  default TermIndexProto toProto() {
    return TermIndexProto.newBuilder()
        .setTerm(getTerm())
        .setIndex(getIndex())
        .build();
  }

  @Override
  default int compareTo(TermIndex that) {
    return Comparator.comparingLong(TermIndex::getTerm)
        .thenComparingLong(TermIndex::getIndex)
        .compare(this, that);
  }

  /**
   * 从 Protobuf 对象创建一个新的 TermIndex 实例。
   * @return a {@link TermIndex} object from the given proto.
   */
  static TermIndex valueOf(TermIndexProto proto) {
    return Optional.ofNullable(proto).map(p -> valueOf(p.getTerm(), p.getIndex())).orElse(null);
  }

  /**
   * 从日志条目对象创建一个新的 TermIndex。
   * @return a {@link TermIndex} object from the given proto.
   */
  static TermIndex valueOf(LogEntryProto proto) {
    return Optional.ofNullable(proto).map(p -> valueOf(p.getTerm(), p.getIndex())).orElse(null);
  }

  /**
   * 创建一个新的 TermIndex 实例，内部通过匿名类实现。
   * @return a {@link TermIndex} object.
   */
  static TermIndex valueOf(long term, long index) {
    return new TermIndex() {
      @Override
      public long getTerm() {
        return term;
      }

      @Override
      public long getIndex() {
        return index;
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == this) {
          return true;
        } else if (!(obj instanceof TermIndex)) {
          return false;
        }

        final TermIndex that = (TermIndex) obj;
        return this.getTerm() == that.getTerm()
            && this.getIndex() == that.getIndex();
      }

      @Override
      public int hashCode() {
        return Long.hashCode(term) ^ Long.hashCode(index);
      }

      private String longToString(long n) {
        return n >= 0L? String.valueOf(n) : "~";
      }

      @Override
      public String toString() {
        return String.format("(t:%s, i:%s)", longToString(term), longToString(index));
      }
    };
  }
}