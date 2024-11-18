
package net.xdob.ratly.protocol;

import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.Preconditions;

public abstract class RaftClientMessage implements RaftRpcMessage {
  private final ClientId clientId;
  private final RaftPeerId serverId;
  private final RaftGroupId groupId;
  private final long callId;

  RaftClientMessage(ClientId clientId, RaftPeerId serverId, RaftGroupId groupId, long callId) {
    this.clientId = Preconditions.assertNotNull(clientId, "clientId");
    this.serverId = Preconditions.assertNotNull(serverId, "serverId");
    this.groupId = Preconditions.assertNotNull(groupId, "groupId");
    this.callId = callId;
  }

  @Override
  public String getRequestorId() {
    return clientId.toString();
  }

  @Override
  public String getReplierId() {
    return serverId.toString();
  }

  public ClientId getClientId() {
    return clientId;
  }

  public RaftPeerId getServerId() {
    return serverId;
  }

  @Override
  public RaftGroupId getRaftGroupId() {
    return groupId;
  }

  public long getCallId() {
    return callId;
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass()) + ":" + clientId + "->" + serverId
        + (groupId != null? "@" + groupId: "") + ", cid=" + getCallId();
  }
}
