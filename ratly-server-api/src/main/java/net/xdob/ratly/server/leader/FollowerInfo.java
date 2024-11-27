
package net.xdob.ratly.server.leader;

import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.LongUnaryOperator;

/**
 * 当前节点是 Leader 时，用于维护和更新 Follower 节点的状态
 */
public interface FollowerInfo {
  Logger LOG = LoggerFactory.getLogger(FollowerInfo.class);

  /** @return the name of this object. */
  String getName();

  /** 返回 Follower 的 RaftPeerId，这是 Follower 节点的唯一标识符
   * @return Follower的RaftPeerId.
   */
  RaftPeerId getId();

  /**
   * Return this follower's {@link RaftPeer}.
   * To obtain the {@link RaftPeerId}, use {@link #getId()} which is more efficient than this method.
   *
   * @return this follower's peer info.
   */
  RaftPeer getPeer();

  /**
   * 返回 Follower 已确认的 matchIndex（即，Follower 已经复制的日志条目的最新索引）
   * @return the matchIndex acknowledged by this follower.
   */
  long getMatchIndex();

  /**
   * 更新 Follower 的 matchIndex。
   */
  boolean updateMatchIndex(long newMatchIndex);

  /**
   * 返回 Follower 已确认的 commitIndex（即 Follower 已经提交的日志条目的索引）。
   * @return the commitIndex acknowledged by this follower.
   */
  long getCommitIndex();

  /**
   * 更新 Follower 的 commitIndex。
   */
  boolean updateCommitIndex(long newCommitIndex);

  /**
   * 返回 Follower 已确认的 snapshotIndex，表示已经应用的快照的索引。
   * @return 已确认的 snapshotIndex
   */
  long getSnapshotIndex();

  /**
   * 设置 Follower 的 snapshotIndex。
   */
  void setSnapshotIndex(long newSnapshotIndex);

  /**
   *  设置Follower 尝试过安装快照。
   */
  void setAttemptedToInstallSnapshot();

  /**
   * 返回是否 Follower 尝试过安装快照。
   * @return 是否 Follower 尝试过安装快照
   */
  boolean hasAttemptedToInstallSnapshot();

  /**
   * 返回 Follower 的 nextIndex，这是 Leader 用来知道下一个要复制到该 Follower 的日志条目的索引。
   * @return the nextIndex for this follower.
   */
  long getNextIndex();

  /**
   * 增加 Follower 的 nextIndex。
   */
  void increaseNextIndex(long newNextIndex);

  /**
   * 减少 Follower 的 nextIndex。
   */
  void decreaseNextIndex(long newNextIndex);

  /**
   * 设置 Follower 的 nextIndex。无条件设置新值
   */
  void setNextIndex(long newNextIndex);

  /**
   * 更新 Follower 的 nextIndex。
   * 只能更新成比老的nextIndex更大的值
   */
  void updateNextIndex(long newNextIndex);

  /**
   * 通过传入的操作符更新 Follower 的 nextIndex。
   */
  void computeNextIndex(LongUnaryOperator op);
  //RPC 时间戳管理

  /**
   * 返回最后一次接收到 RPC 响应的时间戳。
   */
  Timestamp getLastRpcResponseTime();

  /**
   * 返回最后一次发送 RPC 请求的时间戳。
   */
  Timestamp getLastRpcSendTime();

  /**
   * 更新最后一次接收到 RPC 响应的时间戳为当前时间。
   */
  void updateLastRpcResponseTime();

  /**
   * 更新最后一次发送 RPC 请求的时间戳，是否为心跳请求。
   */
  void updateLastRpcSendTime(boolean isHeartbeat);

  /**
   * 返回最后一次发送和接收 RPC 时间中的较晚时间戳。
   */
  Timestamp getLastRpcTime();

  /**
   * 返回最后一次发送心跳的时间戳。
   */
  Timestamp getLastHeartbeatSendTime();

  /**
   * 返回上次响应的 AppendEntries RPC 请求的发送时间戳。
   */
  Timestamp getLastRespondedAppendEntriesSendTime();

  /**
   * 更新 lastRpcResponseTime 和 LastRespondedAppendEntriesSendTime。
   */
  void updateLastRespondedAppendEntriesSendTime(Timestamp sendTime);
}
