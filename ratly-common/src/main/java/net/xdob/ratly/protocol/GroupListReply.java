package net.xdob.ratly.protocol;

import java.util.Collections;
import java.util.List;

/**
 * The response of group list request. Sent from server to client.
 */
public class GroupListReply extends RaftClientReply {

  private final List<RaftGroupId> groupIds;

  public GroupListReply(RaftClientRequest request,  List<RaftGroupId> groupIds) {
    this(request.getClientId(), request.getServerId(), request.getRaftGroupId(), request.getCallId(), groupIds);
  }

  public GroupListReply(ClientId clientId, RaftPeerId serverId, RaftGroupId groupId, long callId,
      List<RaftGroupId> groupIds) {
    super(clientId, serverId, groupId, callId, true, null, null, 0L, null);
    this.groupIds = Collections.unmodifiableList(groupIds);
  }

  public List<RaftGroupId> getGroupIds() {
    return groupIds;
  }
}
