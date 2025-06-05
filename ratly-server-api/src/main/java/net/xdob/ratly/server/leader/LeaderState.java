
package net.xdob.ratly.server.leader;

import net.xdob.ratly.proto.raft.AppendEntriesReplyProto;
import net.xdob.ratly.proto.raft.AppendEntriesRequestProto;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.util.JavaUtils;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * States for leader only.
 * LeaderState 定义了 Leader 节点的核心职责，
 * 特别是在管理与 Follower 交互（如日志复制、心跳检查）和领导状态管理（如降级）方面。
 */
public interface LeaderState {
  /**
   * The reasons that this leader steps down and becomes a follower.
   * 枚举定义了导致 Leader 放弃领导地位并转为 Follower 的原因。
   */
  enum StepDownReason {
    /**
     * 检测到更高的任期。
     */
    HIGHER_TERM,
    /**
     * 存在具有更高优先级的节点。
     */
    HIGHER_PRIORITY,
    /**
     * 无法从多数 Follower 收到心跳响应。
     */
    LOST_MAJORITY_HEARTBEATS,
    /**
     * 状态机运行中发生了异常。
     */
    STATE_MACHINE_EXCEPTION,
    /**
     * JVM 暂停（如 GC 长时间暂停）导致无法正常运行。
     */
    JVM_PAUSE,
    /**
     * 被外部命令强制降级。
     */
    FORCE;

    private final String longName = JavaUtils.getClassSimpleName(getClass()) + ":" + name();

    @Override
    public String toString() {
      return longName;
    }
  }

  /**
   * Restart the given {@link LogAppender}.
   * 重新启动指定的日志追加器（LogAppender）。
   * 日志追加器负责向 Follower 发送日志条目，这个方法通常用于当 LogAppender 出现异常或需要重新初始化时，确保日志复制能够继续进行。
   * @param appender 日志追加器
   */
  void restart(LogAppender appender);

  /**
   * 生成一个新的 AppendEntriesRequestProto 对象，用于向 Follower 发送追加日志的请求。
   * 为日志复制过程中的请求消息提供统一的构造逻辑。
   * @param follower 目标 Follower 的信息。
   * @param entries 需要追加的日志条目。
   * @param previous 上一条日志的 TermIndex 信息，用于日志匹配。
   * @param callId 本次调用的唯一标识符。
   * @return a new {@link AppendEntriesRequestProto} object.
   */
  AppendEntriesRequestProto newAppendEntriesRequestProto(FollowerInfo follower,
      List<LogEntryProto> entries, TermIndex previous, long callId);

  /**
   * @return 当前有效虚节点id
   */
  String getVnPeerId();

  boolean compareAndSetVnPeerId(String expect, String update);

  /**
   * Check if the follower is healthy.
   * 功能：检查指定 Follower 的健康状态。
   * 作用：通过检查心跳响应时间或其他指标，判断 Follower 是否能正常参与日志复制和投票。
   */
  void checkHealth(FollowerInfo follower);

  /**
   * Handle the event that the follower has replied a term.
   * 处理 Follower 返回的任期信息。
   * 作用：如果 followerTerm 大于当前 Leader 的任期，则 Leader 必须降级为 Follower（触发 StepDownReason.HIGHER_TERM）。
   */
  boolean onFollowerTerm(FollowerInfo follower, long followerTerm);

  /**
   * Handle the event that the follower has replied a commit index.
   * 功能：处理 Follower 返回的提交索引信息。
   * 作用：更新 Leader 维护的全局提交索引，确保多数节点已复制的日志能够被提交。
   */
  void onFollowerCommitIndex(FollowerInfo follower, long commitIndex);

  /**
   * Handle the event that the follower has replied a success append entries.
   * 功能：处理 Follower 成功响应日志追加请求的事件。
   * 作用：更新 Follower 的匹配索引 (matchIndex) 和下一个日志索引 (nextIndex)，并检查全局提交索引是否可以推进。
   */
  void onFollowerSuccessAppendEntries(FollowerInfo follower);

  /**
   * Check if a follower is bootstrapping.
   * 功能：检查指定 Follower 是否正在引导（如安装快照以恢复日志）。
   * 作用：用于判断某些操作是否可以延迟或优先处理其他 Follower。
   */
  boolean isFollowerBootstrapping(FollowerInfo follower);

  /**
   * Received an {@link AppendEntriesReplyProto}
   * 功能：处理来自 Follower 的 AppendEntriesReplyProto 回复。
   * 逻辑：
   *    成功的响应需要更新 matchIndex 和 nextIndex。
   *    如果响应失败且由于日志不匹配，可能需要调整 nextIndex 并重试。
   */
  void onAppendEntriesReply(LogAppender appender, AppendEntriesReplyProto reply);

}
