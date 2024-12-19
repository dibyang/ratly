package net.xdob.ratly.server;

import net.xdob.ratly.proto.raft.RaftPeerRole;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

/**
 * Raft集群的配置，具体包含了当前配置和上一个配置。
 * <p>
 * 该接口的主要功能是提供对Raft集群配置的访问，允许用户查询当前配置和以前配置中的节点信息。
 * 配置包括投票成员（如Leader、Candidate、Follower）和非投票成员（listeners）。
 * 它提供了方法来获取与指定角色（如Follower、Leader等）对应的节点集合，并支持查询当前配置和上一个配置。
 * <p>
 * 设计上的注意事项
 *    1.不可变性：该接口描述的是不可变配置，这意味着它的数据一旦创建便不可更改，这有助于保证配置在集群中的一致性和稳定性。
 *    2.角色的支持：通过 RaftPeerRole，不同角色的节点可以在同一集群配置中明确区分，支持 Raft 协议的选举、日志复制等操作。
 *    3.日志索引：通过 getLogEntryIndex()，可以追踪集群配置变更的日志位置，从而保证配置变更与日志条目的一致性。
 *
 * @see net.xdob.ratly.proto.raft.RaftPeerRole
 */
public interface RaftConfiguration {
  /** Is this configuration transitional, i.e. in the middle of a peer change? */
  boolean isTransitional();

  /** Is this configuration stable, i.e. no on-going peer change? */
  boolean isStable();

  @SuppressWarnings({"squid:S6466"})
    // Suppress  ArrayIndexOutOfBoundsException warning
  boolean containsInConf(RaftPeerId peerId, RaftPeerRole... roles);

  PeerConfiguration getConf();

  PeerConfiguration getOldConf();

  boolean isHighestPriority(RaftPeerId peerId);

  boolean containsInOldConf(RaftPeerId peerId);

  /**
   * @return 如果给定的节点包含在 conf 中，并且如果旧 conf 存在，则节点也包含在旧 conf 中。返回true
   */
  boolean containsInBothConfs(RaftPeerId peerId);

  /**
   * 该方法通过给定的节点ID (RaftPeerId) 和角色 (RaftPeerRole) 查找对应的节点信息。如果该节点不在当前配置中，返回 null。
   * @return the peer corresponding to the given id;
   *         or return null if the peer is not in this configuration.
   */
  RaftPeer getPeer(RaftPeerId id, RaftPeerRole... roles);

  /**
   * 这个方法是 getAllPeers(RaftPeerRole.FOLLOWER) 的默认实现，
   * 它会返回当前配置和之前配置中的所有 Follower 类型的节点。
   */
  default List<RaftPeer> getAllPeers() {
    return getAllPeers(RaftPeerRole.FOLLOWER);
  }

  /**
   * 该方法返回当前配置和前一个配置中所有属于指定角色的节点。
   * @return all the peers of the given role in the current configuration and the previous configuration.
   */
  List<RaftPeer> getAllPeers(RaftPeerRole role);

  /**
   * 这是 getCurrentPeers(RaftPeerRole.FOLLOWER) 的默认实现，它返回当前配置中的所有 Follower 类型的节点。
   * The same as getCurrentPeers(RaftPeerRole.FOLLOWER).
   */
  default Collection<RaftPeer> getCurrentPeers() {
    return getCurrentPeers(RaftPeerRole.FOLLOWER);
  }

  /**
   * 该方法返回当前配置中所有属于指定角色的节点。
   * @return all the peers of the given role in the current configuration.
   */
  List<RaftPeer> getCurrentPeers(RaftPeerRole roles);

  /**
   * 这是 getPreviousPeers(RaftPeerRole.FOLLOWER) 的默认实现，返回之前配置中的所有 Follower 类型的节点。
   * The same as getPreviousPeers(RaftPeerRole.FOLLOWER).
   */
  default List<RaftPeer> getPreviousPeers() {
    return getPreviousPeers(RaftPeerRole.FOLLOWER);
  }

  /** @return the peers which are not contained in conf. */
  List<RaftPeer> filterNotContainedInConf(List<RaftPeer> peers);

  /**
   * 该方法返回之前配置中所有属于指定角色的节点。
   * @return all the peers of the given role in the previous configuration.
   */
  List<RaftPeer> getPreviousPeers(RaftPeerRole roles);

  /**
   * @return conf 中除给定的 self id 之外的所有 Peer 节点，和旧的 conf（如果存在）。
   */
  List<RaftPeer> getOtherPeers(RaftPeerId selfId);

  /**
   * @return true if the new peers number reaches half of new conf peers number or the group is
   * changing from single mode to HA mode.
   */
  boolean changeMajority(Collection<RaftPeer> newMembers);

  /**
   * 是否单节点模式
   * @return True if the selfId is in single mode.
   */
  boolean isSingleMode(RaftPeerId selfId);

  /**
   * 是否单节点模式
   * @return true if only one voting member (the leader) in the cluster
   */
  boolean isSingleton();

  /**
   * 是否2节点模式
   */
  boolean isTwoNodeMode();

  /** @return true if the self id together with the others are in the majority. */
  boolean hasMajority(Collection<RaftPeerId> others, RaftPeerId selfId);

  /** @return true if the self id together with the acknowledged followers reach majority. */
  boolean hasMajority(Predicate<RaftPeerId> followers, RaftPeerId selfId);

  @Deprecated
  int getMajorityCount();

  /** @return true if the rejects are in the majority(maybe half is enough in some cases). */
  boolean majorityRejectVotes(Collection<RaftPeerId> rejects);



  boolean hasNoChange(Collection<RaftPeer> newMembers, Collection<RaftPeer> newListeners);

  /**
   * 该方法返回当前配置对应的日志条目索引，表示当前配置在日志中的位置。这个索引通常用来标识当前配置的变更点。
   * @return the index of the corresponding log entry for the current configuration.
   */
  long getLogEntryIndex();
}
