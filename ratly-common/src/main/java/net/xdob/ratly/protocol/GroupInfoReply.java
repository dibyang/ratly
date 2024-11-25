package net.xdob.ratly.protocol;

import net.xdob.ratly.proto.raft.CommitInfoProto;
import net.xdob.ratly.proto.raft.LogInfoProto;
import net.xdob.ratly.proto.raft.RaftConfigurationProto;
import net.xdob.ratly.proto.raft.RoleInfoProto;

import java.util.Collection;
import java.util.Optional;

/**
 * The response of server information request. Sent from server to client.
 */
public class GroupInfoReply extends RaftClientReply {

  private final RaftGroup group;
  private final RoleInfoProto roleInfoProto;
  private final boolean isRaftStorageHealthy;
  private final RaftConfigurationProto conf;
  private final LogInfoProto logInfoProto;

  public GroupInfoReply(RaftClientRequest request, Collection<CommitInfoProto> commitInfos,
      RaftGroup group, RoleInfoProto roleInfoProto, boolean isRaftStorageHealthy,
      RaftConfigurationProto conf, LogInfoProto logInfoProto) {
    this(request.getClientId(), request.getServerId(), request.getRaftGroupId(),
        request.getCallId(), commitInfos,
        group, roleInfoProto, isRaftStorageHealthy, conf, logInfoProto);
  }

  @SuppressWarnings("parameternumber")
  public GroupInfoReply(ClientId clientId, RaftPeerId serverId, RaftGroupId groupId, long callId,
      Collection<CommitInfoProto> commitInfos,
      RaftGroup group, RoleInfoProto roleInfoProto, boolean isRaftStorageHealthy,
      RaftConfigurationProto conf, LogInfoProto logInfoProto) {
    super(clientId, serverId, groupId, callId, true, null, null, 0L, commitInfos);
    this.group = group;
    this.roleInfoProto = roleInfoProto;
    this.isRaftStorageHealthy = isRaftStorageHealthy;
    this.conf = conf;
    this.logInfoProto = logInfoProto;
  }

  public RaftGroup getGroup() {
    return group;
  }

  public RoleInfoProto getRoleInfoProto() {
    return roleInfoProto;
  }

  public boolean isRaftStorageHealthy() {
    return isRaftStorageHealthy;
  }

  public Optional<RaftConfigurationProto> getConf() {
    return Optional.ofNullable(conf);
  }

  public LogInfoProto getLogInfoProto() {
    return logInfoProto;
  }
}
