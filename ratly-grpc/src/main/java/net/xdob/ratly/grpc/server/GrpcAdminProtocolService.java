package net.xdob.ratly.grpc.server;

import net.xdob.ratly.client.impl.ClientProtoUtils;
import net.xdob.ratly.grpc.GrpcUtil;
import net.xdob.ratly.proto.raft.*;
import net.xdob.ratly.protocol.AdminAsynchronousProtocol;
import net.xdob.ratly.protocol.GroupInfoRequest;
import net.xdob.ratly.protocol.GroupListRequest;
import net.xdob.ratly.protocol.GroupManagementRequest;
import net.xdob.ratly.protocol.LeaderElectionManagementRequest;
import net.xdob.ratly.protocol.SetConfigurationRequest;
import net.xdob.ratly.protocol.SnapshotManagementRequest;
import net.xdob.ratly.protocol.TransferLeadershipRequest;
import io.grpc.stub.StreamObserver;
import net.xdob.ratly.proto.raft.RaftClientReplyProto;
import net.xdob.ratly.proto.raft.GroupManagementRequestProto;
import net.xdob.ratly.proto.grpc.AdminProtocolServiceGrpc.AdminProtocolServiceImplBase;

public class GrpcAdminProtocolService extends AdminProtocolServiceImplBase {
  private final AdminAsynchronousProtocol protocol;

  public GrpcAdminProtocolService(AdminAsynchronousProtocol protocol) {
    this.protocol = protocol;
  }

  @Override
  public void groupManagement(GroupManagementRequestProto proto,
        StreamObserver<RaftClientReplyProto> responseObserver) {
    final GroupManagementRequest request = ClientProtoUtils.toGroupManagementRequest(proto);
    GrpcUtil.asyncCall(responseObserver, () -> protocol.groupManagementAsync(request),
        ClientProtoUtils::toRaftClientReplyProto);
  }

  @Override
  public void groupList(GroupListRequestProto proto,
        StreamObserver<GroupListReplyProto> responseObserver) {
    final GroupListRequest request = ClientProtoUtils.toGroupListRequest(proto);
    GrpcUtil.asyncCall(responseObserver, () -> protocol.getGroupListAsync(request),
        ClientProtoUtils::toGroupListReplyProto);
  }

  @Override
  public void groupInfo(GroupInfoRequestProto proto, StreamObserver<GroupInfoReplyProto> responseObserver) {
    final GroupInfoRequest request = ClientProtoUtils.toGroupInfoRequest(proto);
    GrpcUtil.asyncCall(responseObserver, () -> protocol.getGroupInfoAsync(request),
        ClientProtoUtils::toGroupInfoReplyProto);
  }

  @Override
  public void setConfiguration(SetConfigurationRequestProto proto,
      StreamObserver<RaftClientReplyProto> responseObserver) {
    final SetConfigurationRequest request = ClientProtoUtils.toSetConfigurationRequest(proto);
    GrpcUtil.asyncCall(responseObserver, () -> protocol.setConfigurationAsync(request),
        ClientProtoUtils::toRaftClientReplyProto);
  }

  @Override
  public void transferLeadership(TransferLeadershipRequestProto proto,
      StreamObserver<RaftClientReplyProto> responseObserver) {
    final TransferLeadershipRequest request = ClientProtoUtils.toTransferLeadershipRequest(proto);
    GrpcUtil.asyncCall(responseObserver, () -> protocol.transferLeadershipAsync(request),
        ClientProtoUtils::toRaftClientReplyProto);
  }

  @Override
  public void snapshotManagement(SnapshotManagementRequestProto proto,
      StreamObserver<RaftClientReplyProto> responseObserver) {
    final SnapshotManagementRequest request = ClientProtoUtils.toSnapshotManagementRequest(proto);
    GrpcUtil.asyncCall(responseObserver, () -> protocol.snapshotManagementAsync(request),
        ClientProtoUtils::toRaftClientReplyProto);
  }

  @Override
  public void leaderElectionManagement(LeaderElectionManagementRequestProto proto,
      StreamObserver<RaftClientReplyProto> responseObserver) {
    final LeaderElectionManagementRequest request = ClientProtoUtils.toLeaderElectionManagementRequest(proto);
    GrpcUtil.asyncCall(responseObserver, () -> protocol.leaderElectionManagementAsync(request),
        ClientProtoUtils::toRaftClientReplyProto);
  }
}
