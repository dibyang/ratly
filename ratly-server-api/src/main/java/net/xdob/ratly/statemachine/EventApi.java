package net.xdob.ratly.statemachine;

import net.xdob.ratly.proto.raft.*;
import net.xdob.ratly.protocol.RaftGroupMemberId;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;

/**
 * 提供了一个用于事件通知的可选 API，
 * 主要用于通知状态机一些重要事件，比如领导者变更、日志条目更新、配置变化等。
 */
public interface EventApi {
  /**
   * A noop implementation of {@link EventApi}.
   */
  EventApi DEFAULT = new EventApi() {
  };

  /**
   * 通知状态机有新的领导者选举产生。新的领导者可能是当前服务器本身。
   * @param groupMemberId 当前服务器的 ID。
   * @param newLeaderId 新选举出的领导者 ID。
   */
  default void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId newLeaderId) {
  }

  /**
   * 通知状态机日志的 term 和 index 被更新。
   * 这个方法在处理 {@link MetadataProto} 或 {@link RaftConfigurationProto} 时会被调用，
   * 但对于 {@link StateMachineLogEntryProto} 不会触发。
   * Notify the {@link StateMachine} a term-index update event.
   *
   * @param term  日志条目的 term。
   * @param index 日志条目的 index。
   */
  default void notifyTermIndexUpdated(long term, long index) {
  }

  /**
   * 通知状态机 Raft 配置发生变化，通常是在处理 {@link RaftConfigurationProto} 时触发。
   * Notify the {@link StateMachine} a configuration change.
   *
   * @param term                 term of the current log entry
   * @param index                index which is being updated
   * @param newRaftConfiguration new configuration
   */
  default void notifyConfigurationChanged(long term, long index, RaftConfigurationProto newRaftConfiguration) {
  }

  /**
   * 通知状态机群组被移除，通常是在所有挂起的事务都被状态机应用后触发。
   * Notify the {@link StateMachine} a group removal event.
   * This method is invoked after all the pending transactions have been applied by the {@link StateMachine}.
   */
  default void notifyGroupRemove() {
  }

  /**
   * 通知状态机某个日志操作失败。
   * Notify the {@link StateMachine} that a log operation failed.
   *
   * @param cause       The cause of the failure.
   * @param failedEntry The failed log entry, if there is any.
   */
  default void notifyLogFailed(Throwable cause, LogEntryProto failedEntry) {
  }

  /**
   * 通知状态机快照安装完成，可能会触发快照的清理。
   * Notify the {@link StateMachine} that the progress of install snapshot is
   * completely done. Could trigger the cleanup of snapshots.
   *
   * @param result        {@link InstallSnapshotResult}
   * @param snapshotIndex the index of installed snapshot
   * @param peer          the peer who installed the snapshot
   */
  default void notifySnapshotInstalled(InstallSnapshotResult result, long snapshotIndex, RaftPeer peer) {
  }

  /**
   * 通知状态机服务器已经关闭。
   * Notify the {@link StateMachine} that the server for this division has been shut down.
   *
   * @Deprecated please use/override {@link #notifyServerShutdown(RoleInfoProto, boolean)} instead
   */
  @Deprecated
  default void notifyServerShutdown(RoleInfoProto roleInfo) {
    notifyServerShutdown(roleInfo, false);
  }

  /**
   * 通知状态机服务器已经关闭。
   * Notify the {@link StateMachine} that either the server for this division or all the servers have been shut down.
   *
   * @param roleInfo  roleInfo this server
   * @param allServer whether all raft servers will be shutdown at this time
   */
  default void notifyServerShutdown(RoleInfoProto roleInfo, boolean allServer) {
  }
}
