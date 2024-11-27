package net.xdob.ratly.statemachine;

/**
 * 接口定义了一个状态机快照的保留策略，用于决定在系统中保留多少个快照。它为如何管理快照的数量提供了灵活的配置。
 */
public interface SnapshotRetentionPolicy {
  /**
   * 常量，值为 -1，表示保留所有的快照。如果实现类返回 -1，则表示没有限制快照数量，所有快照都将被保留。
   */
  int DEFAULT_ALL_SNAPSHOTS_RETAINED = -1;

  /**
   * 功能：返回应该保留的快照数量。如果返回 -1，则表示保留所有快照；否则，返回一个正整数，表示保留的快照数量。
   * 用途：用于控制系统中状态机快照的数量，可以根据系统的存储容量或其他需求来限制保留的快照数量。例如，如果快照历史很长，可以设置为仅保留最近的若干个快照，避免过多的存储占用。
   * @return -1 for retaining all the snapshots; otherwise, return the number of snapshots to be retained.
   */
  default int getNumSnapshotsRetained() {
    return DEFAULT_ALL_SNAPSHOTS_RETAINED;
  }
}
