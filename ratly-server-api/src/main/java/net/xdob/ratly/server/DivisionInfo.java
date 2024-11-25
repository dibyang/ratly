

package net.xdob.ratly.server;

import net.xdob.ratly.proto.raft.RaftPeerRole;
import net.xdob.ratly.proto.raft.RoleInfoProto;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.util.LifeCycle;

/**
 * Information of a raft server division.
 */
public interface DivisionInfo {
  /** @return the current role of this server division. */
  RaftPeerRole getCurrentRole();

  /** Is this server division currently a follower? */
  default boolean isFollower() {
    return getCurrentRole() == RaftPeerRole.FOLLOWER;
  }

  /** Is this server division currently a candidate? */
  default boolean isCandidate() {
    return getCurrentRole() == RaftPeerRole.CANDIDATE;
  }

  /** Is this server division currently the leader? */
  default boolean isLeader() {
    return getCurrentRole() == RaftPeerRole.LEADER;
  }

  default boolean isListener() {
    return getCurrentRole() == RaftPeerRole.LISTENER;
  }

  /** Is this server division currently the leader and ready? */
  boolean isLeaderReady();

  /**
   * @return the id of the current leader if the leader is known to this server division;
   *         otherwise, return null.
   */
  RaftPeerId getLeaderId();

  /** @return the life cycle state of this server division. */
  LifeCycle.State getLifeCycleState();

  /** Is this server division alive? */
  default boolean isAlive() {
    return !getLifeCycleState().isClosingOrClosed();
  }

  /** @return the role information of this server division. */
  RoleInfoProto getRoleInfoProto();

  /** @return the current term of this server division. */
  long getCurrentTerm();

  /** @return the last log index already applied by the state machine of this server division. */
  long getLastAppliedIndex();

  /**
   * @return an array of next indices of the followers if this server division is the leader;
   *         otherwise, return null.
   */
  long[] getFollowerNextIndices();
}
