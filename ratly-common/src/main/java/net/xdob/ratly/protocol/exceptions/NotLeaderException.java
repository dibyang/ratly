
package net.xdob.ratly.protocol.exceptions;

import net.xdob.ratly.protocol.RaftGroupMemberId;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.util.Preconditions;

import java.util.Collection;
import java.util.Collections;

public class NotLeaderException extends RaftException {
  private final RaftPeer suggestedLeader;
  /** the client may need to update its RaftPeer list */
  private final Collection<RaftPeer> peers;

  public NotLeaderException(RaftGroupMemberId memberId, RaftPeer suggestedLeader, Collection<RaftPeer> peers) {
    super("Server " + memberId + " is not the leader" +
            (suggestedLeader != null ? ", suggested leader is: " + suggestedLeader : ""));
    this.suggestedLeader = suggestedLeader;
    this.peers = peers != null? Collections.unmodifiableCollection(peers): Collections.emptyList();
    Preconditions.assertUnique(this.peers);
  }

  public RaftPeer getSuggestedLeader() {
    return suggestedLeader;
  }

  public Collection<RaftPeer> getPeers() {
    return peers;
  }
}
