package net.xdob.ratly.statemachine;

import net.xdob.ratly.proto.raft.RoleInfoProto;
import net.xdob.ratly.server.config.Notification;
import net.xdob.ratly.server.protocol.TermIndex;

import java.util.concurrent.CompletableFuture;

/**
 * 为仅在从节点（Follower）角色下触发的事件通知设计。
 * 它的目标是通过处理与领导者相关的事件通知（或领导者缺失情况），帮助从节点调整自身状态，从而保持与集群的一致性。
 */
public interface FollowerEventApi {
  /**
   * A noop implementation of {@link FollowerEventApi}.
   */
  FollowerEventApi DEFAULT = new FollowerEventApi() {
  };

  /**
   * 通知状态机在一段时间内，集群中没有领导者存在。这通常表明集群可能处于不稳定状态。
   * 超时时间由配置 raft.server.notification.no-leader.timeout 决定。
   * 可以在状态机中触发额外的操作，例如警告日志、触发诊断或尝试其他恢复逻辑。
   * Notify the {@link StateMachine} that there is no leader in the group for an extended period of time.
   * This notification is based on "raft.server.notification.no-leader.timeout".
   *
   * @param roleInfoProto information about the current node role and rpc delay information
   * @see Notification#NO_LEADER_TIMEOUT_KEY
   */
  default void notifyExtendedNoLeader(RoleInfoProto roleInfoProto) {
  }

  /**
   * 通知状态机领导者已经从日志中清理了一些条目，从节点需要通过异步安装最新快照来进行数据同步。
   * 通常，当从节点的日志落后于领导者且其日志条目无法通过增量复制来更新时，会触发此事件。
   * 安装快照完成后，返回最新快照的最后一个 TermIndex。
   * Notify the {@link StateMachine} that the leader has purged entries from its log.
   * In order to catch up, the {@link StateMachine} has to install the latest snapshot asynchronously.
   *
   * @param roleInfoProto       information about the current node role and rpc delay information.
   * @param firstTermIndexInLog The term-index of the first append entry available in the leader's log.
   * @return return the last term-index in the snapshot after the snapshot installation.
   */
  default CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
      RoleInfoProto roleInfoProto, TermIndex firstTermIndexInLog) {
    return CompletableFuture.completedFuture(null);
  }
}
