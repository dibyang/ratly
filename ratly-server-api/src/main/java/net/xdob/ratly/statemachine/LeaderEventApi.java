package net.xdob.ratly.statemachine;

import net.xdob.ratly.proto.raft.RoleInfoProto;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.server.config.RaftServerConfigKeys;

import java.io.IOException;
import java.util.Collection;

/**
 * 领导者（Leader）相关的事件通知。
 * 只有当当前服务器是 Raft 集群中的领导者时，接口中的方法才会被调用。这些方法主要用于处理领导者角色的状态和与从节点的交互情况。
 */
public interface LeaderEventApi {
  /**
   * A noop implementation of {@link LeaderEventApi}.
   */
  LeaderEventApi DEFAULT = new LeaderEventApi() {
  };

  /**
   * 通知状态机{@link StateMachine}某个从节点（Follower）反应缓慢。这个通知基于配置参数 raft.server.rpc.slowness.timeout。
   * 领导者可以根据此通知采取措施，例如重新调整日志复制策略或记录异常情况。
   *
   * @param leaderInfo   information about the current node role and rpc delay information
   * @param slowFollower The follower being slow.
   * @see RaftServerConfigKeys.Rpc#SLOWNESS_TIMEOUT_KEY
   */
  default void notifyFollowerSlowness(RoleInfoProto leaderInfo, RaftPeer slowFollower) {
  }


  /**
   * 通知状态机{@link StateMachine}当前服务器不再是领导者。
   * 当领导者角色切换时，可能会有一些待处理的事务（pendingEntries），需要状态机妥善处理。
   * 例如，可能需要释放领导者独占的资源或中止未完成的事务。
   */
  default void notifyNotLeader(Collection<TransactionContext> pendingEntries) throws IOException {
  }

  /**
   * 通知状态机{@link StateMachine}当前服务器已经完成领导者角色的初始化并准备就绪。
   * 例如，可以在此时启动领导者特有的任务或服务（如心跳管理、日志同步等）。
   */
  default void notifyLeaderReady() {
  }
}
