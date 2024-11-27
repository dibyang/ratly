package net.xdob.ratly.server;

import net.xdob.ratly.proto.raft.RaftPeerRole;
import net.xdob.ratly.proto.raft.RoleInfoProto;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.util.LifeCycle;

/**
 * 接口提供了与 Raft 协议中服务器分区相关的信息，
 * 特别是该服务器当前角色、生命周期状态、日志信息以及是否处于存活状态等。
 */
public interface DivisionInfo {
  /**
   * 返回当前服务器分区的角色，类型为 RaftPeerRole，可能的值包括：LEADER、FOLLOWER、CANDIDATE、LISTENER 等。
   * 使用场景：用于获取当前服务器分区在 Raft 协议中的角色。
   * @return the current role of this server division.
   */
  RaftPeerRole getCurrentRole();

  /**
   * 检查当前服务器分区是否是 FOLLOWER 角色。
   * 使用场景：当你需要判断该服务器分区是否是跟随者时，可以直接调用此方法。
   */
  default boolean isFollower() {
    return getCurrentRole() == RaftPeerRole.FOLLOWER;
  }

  /**
   * 检查当前服务器分区是否是 CANDIDATE 角色。
   * 使用场景：用于判断该服务器分区是否处于候选人状态，通常在选举期间使用。
   */
  default boolean isCandidate() {
    return getCurrentRole() == RaftPeerRole.CANDIDATE;
  }

  /**
   * 检查当前服务器分区是否是 LEADER 角色。
   * 使用场景：用于判断该服务器分区是否是领导者，通常用于决策和日志管理等任务。
   */
  default boolean isLeader() {
    return getCurrentRole() == RaftPeerRole.LEADER;
  }

  /**
   * 检查当前服务器分区是否是 LISTENER 角色。
   * 使用场景：如果你希望判断某个节点是否是监听者（即没有投票权的成员），可以使用这个方法。
   */
  default boolean isListener() {
    return getCurrentRole() == RaftPeerRole.LISTENER;
  }

  /**
   * 检查当前服务器分区是否处于准备好的领导者状态。
   * 使用场景：在 Raft 协议中，领导者需要准备好才能正常工作，调用此方法可以判断领导者是否准备好接受客户端请求。
   */
  boolean isLeaderReady();

  /**
   * 返回当前服务器分区所知的领导者 ID，如果当前没有领导者则返回 null。
   * 使用场景：当需要了解当前领导者的身份时，可以使用这个方法。如果当前没有领导者，返回值为 null。
   * @return the id of the current leader if the leader is known to this server division;
   *         otherwise, return null.
   */
  RaftPeerId getLeaderId();

  /**
   * 返回当前服务器分区的生命周期状态，类型为 LifeCycle.State。
   * 使用场景：生命周期状态可以反映该服务器分区是否在正常运行，是否正在关闭等。
   * @return the life cycle state of this server division.
   */
  LifeCycle.State getLifeCycleState();

  /**
   * 检查当前服务器分区是否处于存活状态。如果服务器分区的生命周期状态不是关闭或关闭中，返回 true。
   * 使用场景：用于判断该服务器分区是否处于活动状态，特别是用于检测和恢复操作。
   */
  default boolean isAlive() {
    return !getLifeCycleState().isClosingOrClosed();
  }

  /**
   * 返回一个 RoleInfoProto 对象，包含该服务器分区的角色信息。
   * 使用场景：用于获取更详细的角色信息，通常用于日志记录或与外部系统的交互。
   * @return the role information of this server division.
   */
  RoleInfoProto getRoleInfoProto();

  /**
   * 返回当前服务器分区的 Raft 协议中的当前任期（term）。
   * 使用场景：Raft 协议的关键部分是基于任期进行选举和日志一致性的检查，因此获取当前任期非常重要。
   * @return the current term of this server division.
   */
  long getCurrentTerm();

  /**
   * 返回该服务器分区的状态机已应用的最后日志条目的索引。
   * 使用场景：用于跟踪日志的应用进度，确保日志一致性，并确定日志的持久化和提交状态。
   * @return the last log index already applied by the state machine of this server division.
   */
  long getLastAppliedIndex();

  /**
   * 如果当前服务器分区是领导者，返回一个包含各个跟随者的下一个日志索引的数组。否则，返回 null。
   * 使用场景：在领导者节点上，获取各个跟随者的日志进度，用于日志复制。
   * @return an array of next indices of the followers if this server division is the leader;
   *         otherwise, return null.
   */
  long[] getFollowerNextIndices();
}
