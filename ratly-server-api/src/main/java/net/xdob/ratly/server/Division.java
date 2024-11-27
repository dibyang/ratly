package net.xdob.ratly.server;

import com.google.common.collect.Iterables;
import net.xdob.ratly.proto.raft.CommitInfoProto;
import net.xdob.ratly.proto.raft.RaftPeerRole;
import net.xdob.ratly.protocol.RaftGroup;
import net.xdob.ratly.protocol.RaftGroupMemberId;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.metrics.RaftServerMetrics;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.storage.RaftStorage;
import net.xdob.ratly.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collection;
import java.util.Optional;

/**
 * 服务器分区接口
 * RaftServer 的子部分，代表了某个特定 Raft 集群组 (RaftGroup) 中的一个成员
 * A division of a {@link RaftServer} for a particular {@link RaftGroup}.
 */
public interface Division extends Closeable {
  Logger LOG = LoggerFactory.getLogger(Division.class);

  /**
   * 返回该成员的配置属性 {@link DivisionProperties}
   * @return the {@link DivisionProperties} for this division.
   */
  DivisionProperties properties();

  /**
   * @return the {@link RaftGroupMemberId} for this division.
   */
  RaftGroupMemberId getMemberId();

  /**
   * @return the {@link RaftPeerId} for this division.
   */
  default RaftPeerId getId() {
    return getMemberId().getPeerId();
  }

  /**
   * @return the {@link RaftPeer} for this division.
   */
  default RaftPeer getPeer() {
    return Optional.ofNullable(getRaftConf().getPeer(getId(), RaftPeerRole.FOLLOWER, RaftPeerRole.LISTENER))
        .orElseGet(() -> getRaftServer().getPeer());
  }

  /**
   * @return the information about this division.
   */
  DivisionInfo getInfo();

  /**
   * @return the {@link RaftGroup} for this division.
   */
  default RaftGroup getGroup() {
    final Collection<RaftPeer> allFollowerPeers = getRaftConf().getAllPeers(RaftPeerRole.FOLLOWER);
    final Collection<RaftPeer> allListenerPeers = getRaftConf().getAllPeers(RaftPeerRole.LISTENER);
    Iterable<RaftPeer> peers = Iterables.concat(allFollowerPeers, allListenerPeers);
    return RaftGroup.valueOf(getMemberId().getGroupId(), peers);
  }

  /**
   * 返回当前的 Raft 配置 {@link RaftConfiguration} 。
   * @return the current {@link RaftConfiguration} for this division.
   */
  RaftConfiguration getRaftConf();

  /**
   * 返回持有该成员的 {@link RaftServer} 实例。
   * @return the {@link RaftServer} containing this division.
   */
  RaftServer getRaftServer();

  /**
   * 返回该成员的性能指标 ({@link RaftServerMetrics})。
   * @return the {@link RaftServerMetrics} for this division.
   */
  RaftServerMetrics getRaftServerMetrics();

  /**
   * 返回该成员的状态机 {@link StateMachine}。
   * @return the {@link StateMachine} for this division.
   */
  StateMachine getStateMachine();

  /**
   * 返回该成员的 Raft 日志 (RaftLog)。
   * @return the raft log of this division.
   */
  RaftLog getRaftLog();

  /**
   * 返回该成员的存储组件 (RaftStorage)。
   * @return the storage of this division.
   */
  RaftStorage getRaftStorage();

  /**
   * 返回该成员的提交信息 (CommitInfoProto)。
   * @return the commit information of this division.
   */
  Collection<CommitInfoProto> getCommitInfos();

  /**
   * 返回该成员的重试缓存 (RetryCache)。
   * @return the retry cache of this division.
   */
  RetryCache getRetryCache();

  /**
   * 返回该成员的数据流映射 (DataStreamMap)。
   * @return the data stream map of this division.
   */
  DataStreamMap getDataStreamMap();

  /**
   * 返回该成员所属的线程组 (ThreadGroup)。
   * @return the {@link ThreadGroup} the threads of this Division belong to.
   */
  ThreadGroup getThreadGroup();

  @Override
  void close();
}
