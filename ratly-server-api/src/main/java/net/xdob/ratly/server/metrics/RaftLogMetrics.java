
package net.xdob.ratly.server.metrics;

import net.xdob.ratly.server.raftlog.LogEntryHeader;

/** Metrics for a raft log. */
public interface RaftLogMetrics {
  /** A log entry just has been committed. */
  void onLogEntryCommitted(LogEntryHeader header);

  /** Read statemachine data timeout */
  default void onStateMachineDataReadTimeout() {
  }

  /** Write statemachine data timeout */
  default void onStateMachineDataWriteTimeout() {
  }
}
