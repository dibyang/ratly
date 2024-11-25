
package net.xdob.ratly.server.leader;

import net.xdob.ratly.proto.raft.AppendEntriesRequestProto;
import net.xdob.ratly.proto.raft.InstallSnapshotRequestProto;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.RaftServerRpc;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.raftlog.RaftLogIOException;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.util.AwaitForSignal;
import net.xdob.ratly.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * A {@link LogAppender} is for the leader to send appendEntries to a particular follower.
 */
public interface LogAppender {
  Logger LOG = LoggerFactory.getLogger(LogAppender.class);

  Class<? extends LogAppender> DEFAULT_CLASS = ReflectionUtils.getClass(
      LogAppender.class.getName() + "Default", LogAppender.class);

  /** Create the default {@link LogAppender}. */
  static LogAppender newLogAppenderDefault(RaftServer.Division server, LeaderState leaderState, FollowerInfo f) {
    final Class<?>[] argClasses = {RaftServer.Division.class, LeaderState.class, FollowerInfo.class};
    return ReflectionUtils.newInstance(DEFAULT_CLASS, argClasses, server, leaderState, f);
  }

  /** @return the server. */
  RaftServer.Division getServer();

  /** The same as getServer().getRaftServer().getServerRpc(). */
  default RaftServerRpc getServerRpc() {
    return getServer().getRaftServer().getServerRpc();
  }

  /** The same as getServer().getRaftLog(). */
  default RaftLog getRaftLog() {
    return getServer().getRaftLog();
  }

  /** Start this {@link LogAppender}. */
  void start();

  /** Is this {@link LogAppender} running? */
  boolean isRunning();

  /**
   * Stop this {@link LogAppender} asynchronously.
   * @deprecated override {@link #stopAsync()} instead.
   */
  @Deprecated
  default void stop() {
    throw new UnsupportedOperationException();
  }

  /**
   * Stop this {@link LogAppender} asynchronously.
   *
   * @return a future of the final state.
   */
  default CompletableFuture<?> stopAsync() {
    stop();
    return CompletableFuture.supplyAsync(() -> {
      while (isRunning()) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new CompletionException("stopAsync interrupted", e);
        }
      }
      return null;
    });
  }

  /** @return the leader state. */
  LeaderState getLeaderState();

  /** @return the follower information for this {@link LogAppender}. */
  FollowerInfo getFollower();

  /** The same as getFollower().getPeer().getId(). */
  default RaftPeerId getFollowerId() {
    return getFollower().getId();
  }

  /** @return the call id for the next {@link AppendEntriesRequestProto}. */
  long getCallId();

  /** @return the a {@link Comparator} for comparing call ids. */
  Comparator<Long> getCallIdComparator();

  /**
   * Create a {@link AppendEntriesRequestProto} object using the {@link FollowerInfo} of this {@link LogAppender}.
   * The {@link AppendEntriesRequestProto} object may contain zero or more log entries.
   * When there is zero log entries, the {@link AppendEntriesRequestProto} object is a heartbeat.
   *
   * @param callId The call id of the returned request.
   * @param heartbeat the returned request must be a heartbeat.
   *
   * @return a new {@link AppendEntriesRequestProto} object.
   * @deprecated this is no longer a public API.
   */
  @Deprecated
  AppendEntriesRequestProto newAppendEntriesRequest(long callId, boolean heartbeat) throws RaftLogIOException;

  /** @return a new {@link InstallSnapshotRequestProto} object. */
  InstallSnapshotRequestProto newInstallSnapshotNotificationRequest(TermIndex firstAvailableLogTermIndex);

  /** @return an {@link Iterable} of {@link InstallSnapshotRequestProto} for sending the given snapshot. */
  Iterable<InstallSnapshotRequestProto> newInstallSnapshotRequests(String requestId, SnapshotInfo snapshot);

  /**
   * Should this {@link LogAppender} send a snapshot to the follower?
   *
   * @return the snapshot if it should install a snapshot; otherwise, return null.
   */
  default SnapshotInfo shouldInstallSnapshot() {
    // we should install snapshot if the follower needs to catch up and:
    // 1. there is no local log entry but there is snapshot
    // 2. or the follower's next index is smaller than the log start index
    // 3. or the follower is bootstrapping and has not installed any snapshot yet
    final FollowerInfo follower = getFollower();
    final boolean isFollowerBootstrapping = getLeaderState().isFollowerBootstrapping(follower);
    final SnapshotInfo snapshot = getServer().getStateMachine().getLatestSnapshot();

    if (isFollowerBootstrapping && !follower.hasAttemptedToInstallSnapshot()) {
      if (snapshot == null) {
        // Leader cannot send null snapshot to follower. Hence, acknowledge InstallSnapshot attempt (even though it
        // was not attempted) so that follower can come out of staging state after appending log entries.
        follower.setAttemptedToInstallSnapshot();
      } else {
        return snapshot;
      }
    }

    final long followerNextIndex = getFollower().getNextIndex();
    if (followerNextIndex < getRaftLog().getNextIndex()) {
      final long logStartIndex = getRaftLog().getStartIndex();
      if (followerNextIndex < logStartIndex || (logStartIndex == RaftLog.INVALID_LOG_INDEX && snapshot != null)) {
        return snapshot;
      }
    }
    return null;
  }

  /** Define how this {@link LogAppender} should run. */
  void run() throws InterruptedException, IOException;

  /**
   * Get the {@link AwaitForSignal} for events, which can be:
   * (1) new log entries available,
   * (2) log indices changed, or
   * (3) a snapshot installation completed.
   */
  AwaitForSignal getEventAwaitForSignal();

  /** The same as getEventAwaitForSignal().signal(). */
  default void notifyLogAppender() {
    getEventAwaitForSignal().signal();
  }

  /** Should the leader send appendEntries RPC to the follower? */
  default boolean shouldSendAppendEntries() {
    return hasAppendEntries() || getHeartbeatWaitTimeMs() <= 0;
  }

  /** Does it have outstanding appendEntries? */
  default boolean hasAppendEntries() {
    return getFollower().getNextIndex() < getRaftLog().getNextIndex();
  }

  /** Trigger to send a heartbeat AppendEntries. */
  void triggerHeartbeat();

  /** @return the wait time in milliseconds to send the next heartbeat. */
  default long getHeartbeatWaitTimeMs() {
    final int min = getServer().properties().minRpcTimeoutMs();
    // time remaining to send a heartbeat
    final long heartbeatRemainingTimeMs = min/2 - getFollower().getLastRpcResponseTime().elapsedTimeMs();
    // avoid sending heartbeat too frequently
    final long noHeartbeatTimeMs = min/4 - getFollower().getLastHeartbeatSendTime().elapsedTimeMs();
    return Math.max(heartbeatRemainingTimeMs, noHeartbeatTimeMs);
  }

  /** Handle the event that the follower has replied a term. */
  default boolean onFollowerTerm(long followerTerm) {
    synchronized (getServer()) {
      return isRunning() && getLeaderState().onFollowerTerm(getFollower(), followerTerm);
    }
  }
}
