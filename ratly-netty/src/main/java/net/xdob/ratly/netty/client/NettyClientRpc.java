package net.xdob.ratly.netty.client;

import net.xdob.ratly.protocol.GroupListRequest;
import net.xdob.ratly.protocol.GroupManagementRequest;
import net.xdob.ratly.protocol.LeaderElectionManagementRequest;
import net.xdob.ratly.protocol.TransferLeadershipRequest;
import net.xdob.ratly.client.impl.ClientProtoUtils;
import net.xdob.ratly.client.impl.RaftClientRpcWithProxy;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.netty.NettyRpcProxy;
import net.xdob.ratly.protocol.*;
import net.xdob.ratly.proto.raft.*;
import net.xdob.ratly.proto.raft.RaftClientRequestProto;
import net.xdob.ratly.proto.raft.RaftRpcRequestProto;
import net.xdob.ratly.proto.raft.GroupManagementRequestProto;
import net.xdob.ratly.proto.raft.SetConfigurationRequestProto;
import net.xdob.ratly.proto.netty.RaftNettyServerRequestProto;
import net.xdob.ratly.util.JavaUtils;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class NettyClientRpc extends RaftClientRpcWithProxy<NettyRpcProxy> {
  public NettyClientRpc(ClientId clientId, RaftProperties properties) {
    super(new NettyRpcProxy.PeerMap(clientId.toString(), properties));
  }

  @Override
  public CompletableFuture<RaftClientReply> sendRequestAsync(RaftClientRequest request) {
    final RaftPeerId serverId = request.getServerId();
    try {
      final NettyRpcProxy proxy = getProxies().getProxy(serverId);
      final RaftNettyServerRequestProto serverRequestProto = buildRequestProto(request);
      return proxy.sendAsync(serverRequestProto).thenApply(replyProto -> {
        if (request instanceof GroupListRequest) {
          return ClientProtoUtils.toGroupListReply(replyProto.getGroupListReply());
        } else if (request instanceof GroupInfoRequest) {
          return ClientProtoUtils.toGroupInfoReply(replyProto.getGroupInfoReply());
        } else {
          return ClientProtoUtils.toRaftClientReply(replyProto.getRaftClientReply());
        }
      });
    } catch (Throwable e) {
      return JavaUtils.completeExceptionally(e);
    }
  }

  @Override
  public RaftClientReply sendRequest(RaftClientRequest request) throws IOException {
    final RaftPeerId serverId = request.getServerId();
    final NettyRpcProxy proxy = getProxies().getProxy(serverId);

    final RaftNettyServerRequestProto serverRequestProto = buildRequestProto(request);
    final RaftRpcRequestProto rpcRequest = getRpcRequestProto(serverRequestProto);
    if (request instanceof GroupListRequest) {
      return ClientProtoUtils.toGroupListReply(
          proxy.send(rpcRequest, serverRequestProto).getGroupListReply());
    } else if (request instanceof GroupInfoRequest) {
      return ClientProtoUtils.toGroupInfoReply(
          proxy.send(rpcRequest, serverRequestProto).getGroupInfoReply());
    } else {
      return ClientProtoUtils.toRaftClientReply(
          proxy.send(rpcRequest, serverRequestProto).getRaftClientReply());
    }
  }

  private RaftNettyServerRequestProto buildRequestProto(RaftClientRequest request) {
    final RaftNettyServerRequestProto.Builder b = RaftNettyServerRequestProto.newBuilder();
    if (request instanceof GroupManagementRequest) {
      final GroupManagementRequestProto proto = ClientProtoUtils.toGroupManagementRequestProto(
          (GroupManagementRequest)request);
      b.setGroupManagementRequest(proto);
    } else if (request instanceof SetConfigurationRequest) {
      final SetConfigurationRequestProto proto = ClientProtoUtils.toSetConfigurationRequestProto(
          (SetConfigurationRequest)request);
      b.setSetConfigurationRequest(proto);
    } else if (request instanceof GroupListRequest) {
      final GroupListRequestProto proto = ClientProtoUtils.toGroupListRequestProto(
          (GroupListRequest)request);
      b.setGroupListRequest(proto);
    } else if (request instanceof GroupInfoRequest) {
      final GroupInfoRequestProto proto = ClientProtoUtils.toGroupInfoRequestProto(
          (GroupInfoRequest)request);
      b.setGroupInfoRequest(proto);
    } else if (request instanceof TransferLeadershipRequest) {
      final TransferLeadershipRequestProto proto = ClientProtoUtils.toTransferLeadershipRequestProto(
          (TransferLeadershipRequest)request);
      b.setTransferLeadershipRequest(proto);
    } else if (request instanceof SnapshotManagementRequest) {
      final SnapshotManagementRequestProto proto = ClientProtoUtils.toSnapshotManagementRequestProto(
          (SnapshotManagementRequest) request);
      b.setSnapshotManagementRequest(proto);
    } else if (request instanceof LeaderElectionManagementRequest) {
      final LeaderElectionManagementRequestProto proto =
          ClientProtoUtils.toLeaderElectionManagementRequestProto(
          (LeaderElectionManagementRequest) request);
      b.setLeaderElectionManagementRequest(proto);
    } else if (request instanceof ServerAdminRequest) {
			final ServerAdminRequestProto proto = ClientProtoUtils.toServerAdminRequestProto(
					(ServerAdminRequest) request);
			b.setServerAdminRequest(proto);
		} else {
      final RaftClientRequestProto proto = ClientProtoUtils.toRaftClientRequestProto(request);
      b.setRaftClientRequest(proto);
    }
    return b.build();
  }

  private RaftRpcRequestProto getRpcRequestProto(RaftNettyServerRequestProto serverRequestProto) {
    if (serverRequestProto.hasGroupManagementRequest()) {
      return serverRequestProto.getGroupManagementRequest().getRpcRequest();
    } else if (serverRequestProto.hasSetConfigurationRequest()) {
      return serverRequestProto.getSetConfigurationRequest().getRpcRequest();
    } else if (serverRequestProto.hasGroupListRequest()) {
      return serverRequestProto.getGroupListRequest().getRpcRequest();
    } else if (serverRequestProto.hasGroupInfoRequest()) {
      return serverRequestProto.getGroupInfoRequest().getRpcRequest();
    } else {
      return serverRequestProto.getRaftClientRequest().getRpcRequest();
    }
  }
}
