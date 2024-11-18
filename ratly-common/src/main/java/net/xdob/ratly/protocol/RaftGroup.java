
package net.xdob.ratly.protocol;

import net.xdob.ratly.util.Preconditions;

import java.util.*;

/**
 * Description of a raft group, which has a unique {@link RaftGroupId} and a collection of {@link RaftPeer}.
 *
 * The objects of this class are immutable.
 */
public final class RaftGroup {
  private static final RaftGroup EMPTY_GROUP = new RaftGroup();

  public static RaftGroup emptyGroup() {
    return EMPTY_GROUP;
  }

  /** @return a group with the given id and peers. */
  public static RaftGroup valueOf(RaftGroupId groupId, RaftPeer... peers) {
    return new RaftGroup(groupId, peers == null || peers.length == 0? Collections.emptyList(): Arrays.asList(peers));
  }

  /** @return a group with the given id and peers. */
  public static RaftGroup valueOf(RaftGroupId groupId, Iterable<RaftPeer> peers) {
    return new RaftGroup(groupId, peers);
  }

  /** The group id */
  private final RaftGroupId groupId;
  /** The group of raft peers */
  private final Map<RaftPeerId, RaftPeer> peers;

  private RaftGroup() {
    this.groupId = RaftGroupId.emptyGroupId();
    this.peers = Collections.emptyMap();
  }

  private RaftGroup(RaftGroupId groupId, Iterable<RaftPeer> peers) {
    this.groupId = Objects.requireNonNull(groupId, "groupId == null");
    Preconditions.assertTrue(!groupId.equals(EMPTY_GROUP.getGroupId()),
        () -> "Group Id " + groupId + " is reserved for the empty group.");

    if (peers == null || !peers.iterator().hasNext()) {
      this.peers = Collections.emptyMap();
    } else {
      Preconditions.assertUnique(peers);
      final Map<RaftPeerId, RaftPeer> map = new HashMap<>();
      peers.forEach(p -> map.put(p.getId(), p));
      this.peers = Collections.unmodifiableMap(map);
    }
  }

  public RaftGroupId getGroupId() {
    return groupId;
  }

  public Collection<RaftPeer> getPeers() {
    return peers.values();
  }

  /** @return the peer with the given id if it is in this group; otherwise, return null. */
  public RaftPeer getPeer(RaftPeerId id) {
    return peers.get(Objects.requireNonNull(id, "id == null"));
  }

  @Override
  public int hashCode() {
    return groupId.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (!(obj instanceof RaftGroup)) {
      return false;
    }
    final RaftGroup that = (RaftGroup)obj;
    return this.groupId.equals(that.groupId) && this.peers.equals(that.peers);
  }

  @Override
  public String toString() {
    return groupId + ":" + peers.values();
  }
}
