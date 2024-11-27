
package net.xdob.ratly.server.metrics;

import net.xdob.ratly.server.raftlog.LogEntryHeader;

/**
 * 用于收集和记录 Raft 日志的相关指标
 */
public interface RaftLogMetrics {
  /**
   * 功能：当一个日志条目被提交时调用该方法，记录这个事件。
   * 参数：header 是一个 LogEntryHeader 对象，包含有关提交日志条目的元数据（如日志条目的索引、任期等）。
   * 用途：可以用于统计已提交的日志条目的数量、提交延迟等信息。它为 Raft 协议中日志提交的监控提供了基础。
   */
  void onLogEntryCommitted(LogEntryHeader header);

  /**
   * 功能：当状态机数据读取操作发生超时时，触发该方法。
   * 用途：可以记录状态机数据读取的超时事件，例如从磁盘读取状态机快照时发生超时的情况。
   * 默认实现：该方法有一个默认实现，表示它是可选的，不一定需要每次都实现。如果需要监控超时事件，可以覆盖该方法。
   */
  default void onStateMachineDataReadTimeout() {
  }

  /**
   * 功能：当状态机数据写入操作发生超时时，触发该方法。
   * 用途：类似于读取超时，用于记录状态机数据写入超时事件。例如，当将状态机更新写入持久存储时发生超时，可以调用该方法进行记录。
   * 默认实现：同样有默认实现，表示该方法是可选的。
   */
  default void onStateMachineDataWriteTimeout() {
  }
}
