
package net.xdob.ratly.statemachine;

/**
 * Retention policy of state machine snapshots.
 */
public interface SnapshotRetentionPolicy {
  int DEFAULT_ALL_SNAPSHOTS_RETAINED = -1;

  /**
   * @return -1 for retaining all the snapshots; otherwise, return the number of snapshots to be retained.
   */
  default int getNumSnapshotsRetained() {
    return DEFAULT_ALL_SNAPSHOTS_RETAINED;
  }
}
