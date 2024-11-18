
package net.xdob.ratly.protocol;

public class TransferLeadershipRequest extends RaftClientRequest {
  private final RaftPeerId newLeader;

  public TransferLeadershipRequest(
      ClientId clientId, RaftPeerId serverId, RaftGroupId groupId, long callId, RaftPeerId newLeader, long timeoutMs) {
    super(clientId, serverId, groupId, callId, readRequestType(), timeoutMs);
    this.newLeader = newLeader;
  }

  public RaftPeerId getNewLeader() {
    return newLeader;
  }
}