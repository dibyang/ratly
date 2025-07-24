package net.xdob.ratly.statemachine;

import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.RaftLogIOException;

/**
 * 日志查询器
 */
public interface ServerStateSupport {
  long getLastAppliedIndex();
  /**
   * 获取指定日志索引对应的 TermIndex，如果不存在返回 null。
   */
  TermIndex getTermIndex(long index);

  /**
   * 获取指定索引日志
   */
  LogEntryProto get(long index) throws RaftLogIOException;

  /**
   * 获取指定索引或比他小最接近的状态机日志
   */
  LogEntryProto getStateMachineLog(long index) throws RaftLogIOException;
  void stopServerState();
}
