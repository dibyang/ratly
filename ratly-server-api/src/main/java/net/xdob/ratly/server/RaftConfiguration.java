
package net.xdob.ratly.server;

import net.xdob.ratly.proto.raft.RaftPeerRole;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;

import java.util.Collection;

/**
 * A configuration is a subset of the members in a {@link net.xdob.ratly.protocol.RaftGroup}.
 * The configuration of a cluster may change from time to time.
 *
 * In a configuration,
 * - the peers are voting members such as LEADER, CANDIDATE and FOLLOWER;
 * - the listeners are non-voting members.
 *
 * This class captures the current configuration and the previous configuration of a cluster.
 *
 * The objects of this class are immutable.
 *
 * @see net.xdob.ratly.proto.raft.RaftPeerRole
 */
public interface RaftConfiguration {
  /**
   * @return the peer corresponding to the given id;
   *         or return null if the peer is not in this configuration.
   */
  RaftPeer getPeer(RaftPeerId id, RaftPeerRole... roles);

  /** The same as getAllPeers(RaftPeerRole.FOLLOWER). */
  default Collection<RaftPeer> getAllPeers() {
    return getAllPeers(RaftPeerRole.FOLLOWER);
  }

  /** @return all the peers of the given role in the current configuration and the previous configuration. */
  Collection<RaftPeer> getAllPeers(RaftPeerRole role);

  /** The same as getCurrentPeers(RaftPeerRole.FOLLOWER). */
  default Collection<RaftPeer> getCurrentPeers() {
    return getCurrentPeers(RaftPeerRole.FOLLOWER);
  }

  /** @return all the peers of the given role in the current configuration. */
  Collection<RaftPeer> getCurrentPeers(RaftPeerRole roles);

  /** The same as getPreviousPeers(RaftPeerRole.FOLLOWER). */
  default Collection<RaftPeer> getPreviousPeers() {
    return getPreviousPeers(RaftPeerRole.FOLLOWER);
  }

  /** @return all the peers of the given role in the previous configuration. */
  Collection<RaftPeer> getPreviousPeers(RaftPeerRole roles);

  /** @return the index of the corresponding log entry for the current configuration. */
  long getLogEntryIndex();
}
