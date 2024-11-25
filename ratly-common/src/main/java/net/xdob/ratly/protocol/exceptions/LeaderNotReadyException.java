package net.xdob.ratly.protocol.exceptions;

import net.xdob.ratly.proto.raft.RaftGroupMemberIdProto;
import net.xdob.ratly.protocol.RaftGroupMemberId;
import net.xdob.ratly.util.ProtoUtils;

/**
 * This exception is sent from the server to a client. The server has just
 * become the current leader, but has not committed its first place-holder
 * log entry yet. Thus the leader cannot accept any new client requests since
 * it cannot determine whether a request is just a retry.
 */
public class LeaderNotReadyException extends ServerNotReadyException {
  private final RaftGroupMemberIdProto serverId;

  public LeaderNotReadyException(RaftGroupMemberId id) {
    super(id + " is in LEADER state but not ready yet.");
    this.serverId = ProtoUtils.toRaftGroupMemberIdProtoBuilder(id).build();
  }

  public RaftGroupMemberId getServerId() {
    return ProtoUtils.toRaftGroupMemberId(serverId);
  }

  public RaftGroupMemberIdProto getRaftGroupMemberIdProto() {
    return serverId;
  }
}
