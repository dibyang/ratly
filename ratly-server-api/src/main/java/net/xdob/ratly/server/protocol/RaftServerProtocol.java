package net.xdob.ratly.server.protocol;

import java.io.IOException;

import net.xdob.ratly.proto.raft.AppendEntriesReplyProto;
import net.xdob.ratly.proto.raft.AppendEntriesRequestProto;
import net.xdob.ratly.proto.raft.InstallSnapshotReplyProto;
import net.xdob.ratly.proto.raft.InstallSnapshotRequestProto;
import net.xdob.ratly.proto.raft.RequestVoteReplyProto;
import net.xdob.ratly.proto.raft.RequestVoteRequestProto;
import net.xdob.ratly.proto.raft.StartLeaderElectionReplyProto;
import net.xdob.ratly.proto.raft.StartLeaderElectionRequestProto;

/**
 * Raft 分布式一致性协议中服务器节点之间通信的核心接口，定义了节点间的主要 RPC 方法。这些方法用于实现 Raft 协议的关键操作，如选举、日志复制和快照安装。
 */
public interface RaftServerProtocol {
  /**
   * 用于标识不同的 RPC 操作类型。
   * 可能用于日志记录、监控或动态处理不同操作。
   */
  enum Op {
    /**
     * 请求投票，触发领导者选举。
     */
    REQUEST_VOTE,
    /**
     * 日志复制请求，用于保持日志一致性。
     */
    APPEND_ENTRIES,
    /**
     * 安装快照，用于日志截断或新节点的初始化。
     */
    INSTALL_SNAPSHOT}

  /**
   * 处理来自其他节点的投票请求。
   * 当某节点试图成为领导者时，会向集群中其他节点发送 RequestVote 请求。
   * @param request 包含候选节点的任期、候选者 ID、候选者的日志最新索引和任期等信息。
   * @return 表示投票结果，包括当前节点的任期和是否同意投票。
   */
  RequestVoteReplyProto requestVote(RequestVoteRequestProto request) throws IOException;

  /**
   * 处理日志追加请求。
   * 由领导者发送给跟随者，用于复制日志条目或发送心跳信号。
   * @param request 包含领导者任期、领导者 ID、前一个日志条目的索引和任期、日志条目数组（可选）、领导者已提交的日志索引等信息。
   * @return 跟随者的响应，指示是否成功追加日志，以及跟随者的当前任期。
   */
  AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto request) throws IOException;

  /**
   * 处理快照安装请求。
   * 领导者将其快照发送给需要同步的跟随者，以减少日志传输的开销。
   * @param request 包含领导者任期、领导者 ID、快照的元数据和数据块。
   * @return 快照安装的响应，指示是否成功。
   */
  InstallSnapshotReplyProto installSnapshot(InstallSnapshotRequestProto request) throws IOException;

  /**
   * 处理领导者选举的启动请求。
   * 用于支持自定义选举机制或触发选举事件。
   * @param request 包含启动选举的相关上下文信息。
   * @return 选举启动的响应。
   */
  StartLeaderElectionReplyProto startLeaderElection(StartLeaderElectionRequestProto request) throws IOException;
}
