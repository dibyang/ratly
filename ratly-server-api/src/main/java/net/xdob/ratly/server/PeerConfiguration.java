package net.xdob.ratly.server;

import net.xdob.ratly.proto.raft.RaftPeerRole;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * The peer configuration of a raft cluster.
 * <p>
 * The objects of this class are immutable.
 */
public class PeerConfiguration {
  /**
   * Peers are voting members such as LEADER, CANDIDATE and FOLLOWER
   * @see net.xdob.ratly.proto.raft.RaftPeerRole
   */
  private final Map<RaftPeerId, RaftPeer> peers;
  /**
   * Listeners are non-voting members.
   * @see net.xdob.ratly.proto.raft.RaftPeerRole#LISTENER
   */
  private final Map<RaftPeerId, RaftPeer> listeners;

  static Map<RaftPeerId, RaftPeer> newMap(Iterable<RaftPeer> peers, String name, Map<RaftPeerId, RaftPeer> existing) {
    Objects.requireNonNull(peers, () -> name + " == null");
    final Map<RaftPeerId, RaftPeer> map = new HashMap<>();
    for(RaftPeer p : peers) {
      if (existing.containsKey(p.getId())) {
        throw new IllegalArgumentException("Failed to initialize " + name
            + ": Found " + p.getId() + " in existing peers " + existing);
      }
      final RaftPeer previous = map.putIfAbsent(p.getId(), p);
      if (previous != null) {
        throw new IllegalArgumentException("Failed to initialize " + name
            + ": Found duplicated ids " + p.getId() + " in " + peers);
      }
    }
    return Collections.unmodifiableMap(map);
  }

  public PeerConfiguration(Iterable<RaftPeer> peers) {
    this(peers, Collections.emptyList());
  }

  public PeerConfiguration(Iterable<RaftPeer> peers, Iterable<RaftPeer> listeners) {
    this.peers = newMap(peers, "peers", Collections.emptyMap());
    this.listeners = Optional.ofNullable(listeners)
        .map(l -> newMap(listeners, "listeners", this.peers))
        .orElseGet(Collections::emptyMap);
  }

  private Map<RaftPeerId, RaftPeer> getPeerMap(RaftPeerRole r) {
    if (r == RaftPeerRole.FOLLOWER) {
      return peers;
    } else if (r == RaftPeerRole.LISTENER) {
      return listeners;
    } else {
      throw new IllegalArgumentException("Unexpected RaftPeerRole " + r);
    }
  }

  public List<RaftPeer> getPeers(RaftPeerRole role) {
    return Collections.unmodifiableList(new ArrayList<>(getPeerMap(role).values()));
  }

  public int size() {
    return peers.size();
  }

  public Stream<RaftPeerId> streamPeerIds() {
    return peers.keySet().stream();
  }

  @Override
  public String toString() {
    return "peers:" + peers.values() + "|listeners:" + listeners.values();
  }

  public RaftPeer getPeer(RaftPeerId id, RaftPeerRole... roles) {
    if (roles == null || roles.length == 0) {
      return peers.get(id);
    }
    for(RaftPeerRole r : roles) {
      final RaftPeer peer = getPeerMap(r).get(id);
      if (peer != null) {
        return peer;
      }
    }
    return null;
  }

  public boolean contains(RaftPeerId id) {
    return contains(id, RaftPeerRole.FOLLOWER);
  }

  public boolean contains(RaftPeerId id, RaftPeerRole r) {
    return getPeerMap(r).containsKey(id);
  }

  public RaftPeerRole contains(RaftPeerId id, EnumSet<RaftPeerRole> roles) {
    if (roles == null || roles.isEmpty()) {
      return peers.containsKey(id)? RaftPeerRole.FOLLOWER: null;
    }
    for(RaftPeerRole r : roles) {
      if (getPeerMap(r).containsKey(id)) {
        return r;
      }
    }
    return null;
  }

  public List<RaftPeer> getOtherPeers(RaftPeerId selfId) {
    List<RaftPeer> others = new ArrayList<>();
    for (Map.Entry<RaftPeerId, RaftPeer> entry : peers.entrySet()) {
      if (!selfId.equals(entry.getValue().getId())) {
        others.add(entry.getValue());
      }
    }
    return others;
  }

  public boolean hasMajority(Collection<RaftPeerId> others, RaftPeerId selfId) {
    Preconditions.assertTrue(!others.contains(selfId));
    return hasMajority(others::contains, contains(selfId));
  }

  public boolean hasMajority(Predicate<RaftPeerId> activePeers, boolean includeSelf) {
    if (peers.isEmpty() && !includeSelf) {
      return true;
    }

    int num = includeSelf ? 1 : 0;
    for (RaftPeerId peerId: peers.keySet()) {
      if (activePeers.test(peerId)) {
        num++;
      }
    }
    return num >= getMajorityCount();
  }

  public int getMajorityCount() {
    if(size()==2){
      return 1;
    }
    return size() / 2 + 1;
  }

  public boolean majorityRejectVotes(Collection<RaftPeerId> rejected) {
    int num = size();
    for (RaftPeerId other : rejected) {
      if (contains(other)) {
        num --;
      }
    }
    return num <= size() / 2;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    final PeerConfiguration that = (PeerConfiguration)obj;
    return this.peers.equals(that.peers);
  }

  @Override
  public int hashCode() {
    return peers.keySet().hashCode(); // hashCode of a set is well defined in Java.
  }
}
