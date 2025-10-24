
package net.xdob.ratly.server.leader;

import net.xdob.ratly.proto.raft.AppendEntriesRequestProto;
import net.xdob.ratly.proto.raft.InstallSnapshotRequestProto;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.Division;
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
 * Raft 协议中用于领导者（Leader）向特定追随者（Follower）发送 AppendEntries 请求的组件，其职责包括日志条目复制、心跳维护和快照安装等。
 * 核心职责
 *    1.日志复制：生成和发送日志条目，确保一致性。
 *    2.心跳维护：通过空的 AppendEntries 请求检测追随者是否存活。
 *    3.快照安装：在日志不足以满足追随者同步时发送快照。
 *    4.运行状态管理：启动、停止和运行日志追加器。
 */
public interface LogAppender {
  Logger LOG = LoggerFactory.getLogger(LogAppender.class);

  Class<? extends LogAppender> DEFAULT_CLASS = ReflectionUtils.getClass(
      LogAppender.class.getName() + "Default", LogAppender.class);

  /**
   * Create the default {@link LogAppender}.
   *
   */
  static LogAppender newLogAppenderDefault(Division server, LeaderState leaderState, FollowerInfo f) {
    final Class<?>[] argClasses = {Division.class, LeaderState.class, FollowerInfo.class};
    return ReflectionUtils.newInstance(DEFAULT_CLASS, argClasses, server, leaderState, f);
  }

  /** @return the server. */
  Division getServer();

  /** The same as getServer().getRaftServer().getServerRpc(). */
  default RaftServerRpc getServerRpc() {
    return getServer().getRaftServer().getServerRpc();
  }

  /** The same as getServer().getRaftLog(). */
  default RaftLog getRaftLog() {
    return getServer().getRaftLog();
  }

  /**
   * Start this {@link LogAppender}.
   * 启动日志追加器
   */
  void start();

  /**
   * Is this {@link LogAppender} running?
   * 检查其是否在运行
   */
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
   * 异步停止日志追加器，返回一个 CompletableFuture，等待停止完成。
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

  /**
   * 创建一个快照通知请求，通知 Follower 开始快照安装。
   * @param firstAvailableLogTermIndex 快照中第一个可用日志条目的 TermIndex。
   * @return a new {@link InstallSnapshotRequestProto} object.
   */
  InstallSnapshotRequestProto newInstallSnapshotNotificationRequest(TermIndex firstAvailableLogTermIndex);


  /**
   * 创建一组快照请求，用于分块发送快照数据。
   * @param requestId 唯一标识符。
   * @param snapshot 要发送的快照。
   * @return an {@link Iterable} of {@link InstallSnapshotRequestProto} for sending the given snapshot.
   */
  Iterable<InstallSnapshotRequestProto> newInstallSnapshotRequests(String requestId, SnapshotInfo snapshot);

  /**
   * Should this {@link LogAppender} send a snapshot to the follower?
   *
   * @return the snapshot if it should install a snapshot; otherwise, return null.
   * 功能：决定是否需要安装快照。
   * 逻辑：
   *    1.如果 Follower 正在引导且未尝试安装快照，优先返回快照。
   *    2.如果 Follower 的 nextIndex 小于日志的 startIndex，也需要发送快照。
   * 返回值：需要安装的快照信息或 null。
   */
  default SnapshotInfo shouldInstallSnapshot() {

		// 当从节点需要追赶上进度时，若满足以下任一条件则应安装快照：
		// 1. 本地不存在日志条目但存在快照
		// 2. 从节点的下一索引值小于日志起始索引
		// 3. 从节点正在启动且尚未安装任何快照
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

  /** Define how this {@link LogAppender} should run.
   * 定义日志追加器的运行逻辑。
   */
  void run() throws InterruptedException, IOException;


	/**
	 * 获取用于等待事件信号的{@link AwaitForSignal}，这些事件可能包括：
	 * (1) 有新的日志条目可用
	 * (2) 日志索引发生变更
	 * (3) 快照安装完成
	 */
  AwaitForSignal getEventAwaitForSignal();

  /**
   * The same as getEventAwaitForSignal().signal().
   * 唤醒日志追加器，使其能够及时处理新的日志条目或状态变更。
   */
  default void notifyLogAppender() {
    getEventAwaitForSignal().signal();
  }

  /**
   * Should the leader send appendEntries RPC to the follower?
   * 功能：判断是否需要发送 AppendEntries 请求。
   * 逻辑：
   *    如果存在未发送的日志条目，返回 true。
   *    如果到下一次心跳的等待时间为零或以下，返回 true。
   */
  default boolean shouldSendAppendEntries() {
    return hasAppendEntries() || getHeartbeatWaitTimeMs() <= 0;
  }

  /**
   * Does it have outstanding appendEntries?
   * 功能：判断是否有未发送的日志条目。
   * 逻辑：检查 Follower 的 nextIndex 是否小于当前日志的 nextIndex。
   */
  default boolean hasAppendEntries() {
    return getFollower().getNextIndex() < getRaftLog().getNextIndex();
  }

  /** Trigger to send a heartbeat AppendEntries.
   * 立即触发一次心跳发送，确保 Follower 保持活跃。
   */
  void triggerHeartbeat();

  /**
   * 功能：计算到下一次心跳发送的等待时间。
   * 逻辑：基于 RPC 最小超时时间计算。避免频繁发送心跳。
   * @return the wait time in milliseconds to send the next heartbeat.
   */
  default long getHeartbeatWaitTimeMs() {
    final int min = getServer().properties().minRpcTimeoutMs();
    // time remaining to send a heartbeat
    final long heartbeatRemainingTimeMs = min/2 - getFollower().getLastRpcResponseTime().elapsedTimeMs();
    // avoid sending heartbeat too frequently
    final long noHeartbeatTimeMs = min/4 - getFollower().getLastHeartbeatSendTime().elapsedTimeMs();
    return Math.max(heartbeatRemainingTimeMs, noHeartbeatTimeMs);
  }

  /**
   * Handle the event that the follower has replied a term.
   * 功能：处理 Follower 返回的任期信息。
   * 逻辑：如果 Follower 的任期大于当前 Leader 的任期，触发降级逻辑。
   */
  default boolean onFollowerTerm(long followerTerm) {
    synchronized (getServer()) {
      return isRunning() && getLeaderState().onFollowerTerm(getFollower(), followerTerm);
    }
  }
}
