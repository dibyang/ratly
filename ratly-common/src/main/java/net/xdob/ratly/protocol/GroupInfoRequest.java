package net.xdob.ratly.protocol;

/**
 * Client sends this request to a server to request for the information about
 * the server itself.
 */
public class GroupInfoRequest extends RaftClientRequest {
  public GroupInfoRequest(ClientId clientId, RaftPeerId serverId, RaftGroupId groupId, long callId) {
    super(clientId, serverId, groupId, callId, readRequestType());
  }
}
