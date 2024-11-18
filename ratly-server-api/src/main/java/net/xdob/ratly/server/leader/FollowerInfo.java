
package net.xdob.ratly.server.leader;

import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.LongUnaryOperator;

/**
 * Information of a follower, provided the local server is the Leader
 */
public interface FollowerInfo {
  Logger LOG = LoggerFactory.getLogger(FollowerInfo.class);

  /** @return the name of this object. */
  String getName();

  /** @return this follower's peer id. */
  RaftPeerId getId();

  /**
   * Return this follower's {@link RaftPeer}.
   * To obtain the {@link RaftPeerId}, use {@link #getId()} which is more efficient than this method.
   *
   * @return this follower's peer info.
   */
  RaftPeer getPeer();

  /** @return the matchIndex acknowledged by this follower. */
  long getMatchIndex();

  /** Update this follower's matchIndex. */
  boolean updateMatchIndex(long newMatchIndex);

  /** @return the commitIndex acknowledged by this follower. */
  long getCommitIndex();

  /** Update follower's commitIndex. */
  boolean updateCommitIndex(long newCommitIndex);

  /** @return the snapshotIndex acknowledged by this follower. */
  long getSnapshotIndex();

  /** Set follower's snapshotIndex. */
  void setSnapshotIndex(long newSnapshotIndex);

  /** Acknowledge that Follower attempted to install a snapshot. It does not guarantee that the installation was
   * successful. This helps to determine whether Follower can come out of bootstrap process. */
  void setAttemptedToInstallSnapshot();

  /** Return true if install snapshot has been attempted by the Follower at least once. Used to verify if
   * Follower tried to install snapshot during bootstrap process. */
  boolean hasAttemptedToInstallSnapshot();

  /** @return the nextIndex for this follower. */
  long getNextIndex();

  /** Increase the nextIndex for this follower. */
  void increaseNextIndex(long newNextIndex);

  /** Decrease the nextIndex for this follower. */
  void decreaseNextIndex(long newNextIndex);

  /** Set the nextIndex for this follower. */
  void setNextIndex(long newNextIndex);

  /** Update the nextIndex for this follower. */
  void updateNextIndex(long newNextIndex);

  /** Set the nextIndex for this follower. */
  void computeNextIndex(LongUnaryOperator op);

  /** @return the lastRpcResponseTime . */
  Timestamp getLastRpcResponseTime();

  /** @return the lastRpcSendTime . */
  Timestamp getLastRpcSendTime();

  /** Update lastRpcResponseTime to the current time. */
  void updateLastRpcResponseTime();

  /** Update lastRpcSendTime to the current time. */
  void updateLastRpcSendTime(boolean isHeartbeat);

  /** @return the latest of the lastRpcSendTime and the lastRpcResponseTime . */
  Timestamp getLastRpcTime();

  /** @return the latest heartbeat send time. */
  Timestamp getLastHeartbeatSendTime();

  /** @return the send time of last responded rpc */
  Timestamp getLastRespondedAppendEntriesSendTime();

  /** Update lastRpcResponseTime and LastRespondedAppendEntriesSendTime */
  void updateLastRespondedAppendEntriesSendTime(Timestamp sendTime);
}
