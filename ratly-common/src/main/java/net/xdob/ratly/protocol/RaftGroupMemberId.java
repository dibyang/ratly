
package net.xdob.ratly.protocol;

import java.util.Objects;

/**
 * A {@link RaftGroupMemberId} consists of a {@link RaftPeerId} and a {@link RaftGroupId}.
 *
 * This is a value-based class.
 */
public final class RaftGroupMemberId {
  public static RaftGroupMemberId valueOf(RaftPeerId peerId, RaftGroupId groupId) {
    return new RaftGroupMemberId(peerId, groupId);
  }

  private final RaftPeerId peerId;
  private final RaftGroupId groupId;
  private final String name;

  private RaftGroupMemberId(RaftPeerId peerId, RaftGroupId groupId) {
    this.peerId = Objects.requireNonNull(peerId, "peerId == null");
    this.groupId = Objects.requireNonNull(groupId, "groupId == null");
    this.name = peerId + "@" + groupId;
  }

  public RaftPeerId getPeerId() {
    return peerId;
  }

  public RaftGroupId getGroupId() {
    return groupId;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (!(obj instanceof RaftGroupMemberId)) {
      return false;
    }

    final RaftGroupMemberId that = (RaftGroupMemberId)obj;
    return this.peerId.equals(that.peerId) && this.groupId.equals(that.groupId);
  }

  @Override
  public int hashCode() {
    return peerId.hashCode() ^ groupId.hashCode();
  }
}
