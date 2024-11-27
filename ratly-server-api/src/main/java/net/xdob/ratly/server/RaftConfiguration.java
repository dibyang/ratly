package net.xdob.ratly.server;

import net.xdob.ratly.proto.raft.RaftPeerRole;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;

import java.util.Collection;

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
  default Collection<RaftPeer> getAllPeers() {
    return getAllPeers(RaftPeerRole.FOLLOWER);
  }

  /**
   * 该方法返回当前配置和前一个配置中所有属于指定角色的节点。
   * @return all the peers of the given role in the current configuration and the previous configuration.
   */
  Collection<RaftPeer> getAllPeers(RaftPeerRole role);

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
  Collection<RaftPeer> getCurrentPeers(RaftPeerRole roles);

  /**
   * 这是 getPreviousPeers(RaftPeerRole.FOLLOWER) 的默认实现，返回之前配置中的所有 Follower 类型的节点。
   * The same as getPreviousPeers(RaftPeerRole.FOLLOWER).
   */
  default Collection<RaftPeer> getPreviousPeers() {
    return getPreviousPeers(RaftPeerRole.FOLLOWER);
  }

  /**
   * 该方法返回之前配置中所有属于指定角色的节点。
   * @return all the peers of the given role in the previous configuration.
   */
  Collection<RaftPeer> getPreviousPeers(RaftPeerRole roles);

  /**
   * 该方法返回当前配置对应的日志条目索引，表示当前配置在日志中的位置。这个索引通常用来标识当前配置的变更点。
   * @return the index of the corresponding log entry for the current configuration.
   */
  long getLogEntryIndex();
}
