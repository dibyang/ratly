
package net.xdob.ratly.statemachine;

import net.xdob.ratly.server.storage.RaftStorage;

import java.io.File;
import java.io.IOException;

public interface StateMachineStorage {
  /**
   * 功能: 初始化存储。接收一个 RaftStorage 对象，
   * 它可能用于配置或提供与 Raft 协议相关的存储功能（如日志、快照等）。
   * 抛出 IOException 表明存储初始化过程可能会出错。
   */
  void init(RaftStorage raftStorage) throws IOException;

  /**
   * 返回最新的持久化快照信息。快照用于 Raft 协议中的状态持久化，保证即使发生崩溃，也能恢复到最近的一致状态。
   * 此方法返回一个 SnapshotInfo 对象，包含快照的相关信息（如快照的版本、时间戳等）。
   */
  SnapshotInfo getLatestSnapshot();


  /**
   * 格式化存储，通常用于清空或重置存储的状态。这个方法可能会在初始化时或需要清理存储时调用。
   */
  void format() throws IOException;

  /**
   * 清理旧的快照。Raft 协议中，通常会保留多个历史快照，以便在出现故障时能够恢复。
   * 这个方法根据提供的 SnapshotRetentionPolicy 来决定如何删除不再需要的快照（例如基于时间或数量限制）。
   */
  void cleanupOldSnapshots(SnapshotRetentionPolicy snapshotRetentionPolicy) throws IOException;

  /**
   * @return the state machine directory.
   */
  default File getSnapshotDir() {
    return null;
  }

  /**
   * @return the temporary directory.
   */
  default File getTmpDir() {
    return null;
  }
}
