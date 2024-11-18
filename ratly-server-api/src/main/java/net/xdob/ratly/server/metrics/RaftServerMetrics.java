

package net.xdob.ratly.server.metrics;

/** Metrics for a raft Server. */
public interface RaftServerMetrics {
  /** A snapshot just has been installed. */
  void onSnapshotInstalled();
}