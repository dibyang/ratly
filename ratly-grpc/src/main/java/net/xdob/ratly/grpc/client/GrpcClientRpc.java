
package net.xdob.ratly.grpc.client;

import net.xdob.ratly.protocol.GroupListRequest;
import net.xdob.ratly.protocol.GroupManagementRequest;
import net.xdob.ratly.protocol.LeaderElectionManagementRequest;
import net.xdob.ratly.protocol.TransferLeadershipRequest;
import net.xdob.ratly.client.impl.ClientProtoUtils;
import net.xdob.ratly.client.impl.RaftClientRpcWithProxy;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.grpc.GrpcConfigKeys;
import net.xdob.ratly.grpc.GrpcTlsConfig;
import net.xdob.ratly.grpc.GrpcUtil;
import net.xdob.ratly.protocol.*;
import net.xdob.ratly.protocol.exceptions.AlreadyClosedException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import net.xdob.ratly.proto.raft.GroupInfoRequestProto;
import net.xdob.ratly.proto.raft.GroupListRequestProto;
import net.xdob.ratly.proto.raft.GroupManagementRequestProto;
import net.xdob.ratly.proto.raft.RaftClientReplyProto;
import net.xdob.ratly.proto.raft.RaftClientRequestProto;
import net.xdob.ratly.proto.raft.SetConfigurationRequestProto;
import net.xdob.ratly.proto.raft.TransferLeadershipRequestProto;
import net.xdob.ratly.proto.raft.SnapshotManagementRequestProto;
import net.xdob.ratly.proto.raft.LeaderElectionManagementRequestProto;
import net.xdob.ratly.util.IOUtils;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.PeerProxyMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class GrpcClientRpc extends RaftClientRpcWithProxy<GrpcClientProtocolClient> {
  public static final Logger LOG = LoggerFactory.getLogger(GrpcClientRpc.class);

  private final ClientId clientId;
  private final int maxMessageSize;

  public GrpcClientRpc(ClientId clientId, RaftProperties properties,
      GrpcTlsConfig adminTlsConfig, GrpcTlsConfig clientTlsConfig) {
    super(new PeerProxyMap<>(clientId.toString(),
        p -> new GrpcClientProtocolClient(clientId, p, properties, adminTlsConfig, clientTlsConfig)));
    this.clientId = clientId;
    this.maxMessageSize = GrpcConfigKeys.messageSizeMax(properties, LOG::debug).getSizeInt();
  }

  @Override
  public CompletableFuture<RaftClientReply> sendRequestAsync(
      RaftClientRequest request) {
    final RaftPeerId serverId = request.getServerId();
    try {
      final GrpcClientProtocolClient proxy = getProxies().getProxy(serverId);
      // Reuse the same grpc stream for all async calls.
      return proxy.getOrderedStreamObservers().onNext(request);
    } catch (Exception e) {
      return JavaUtils.completeExceptionally(e);
    }
  }

  @Override
  public CompletableFuture<RaftClientReply> sendRequestAsyncUnordered(RaftClientRequest request) {
    final RaftPeerId serverId = request.getServerId();
    try {
      final GrpcClientProtocolClient proxy = getProxies().getProxy(serverId);
      // Reuse the same grpc stream for all async calls.
      return proxy.getUnorderedAsyncStreamObservers().onNext(request);
    } catch (Exception e) {
      LOG.error(clientId + ": Failed " + request, e);
      return JavaUtils.completeExceptionally(e);
    }
  }

  @Override
  public RaftClientReply sendRequest(RaftClientRequest request)
      throws IOException {
    final RaftPeerId serverId = request.getServerId();
    final GrpcClientProtocolClient proxy = getProxies().getProxy(serverId);
    if (request instanceof GroupManagementRequest) {
      final GroupManagementRequestProto proto = ClientProtoUtils.toGroupManagementRequestProto(
          (GroupManagementRequest)request);
      return ClientProtoUtils.toRaftClientReply(proxy.groupAdd(proto));
    } else if (request instanceof SetConfigurationRequest) {
      final SetConfigurationRequestProto setConf = ClientProtoUtils.toSetConfigurationRequestProto(
          (SetConfigurationRequest) request);
      return ClientProtoUtils.toRaftClientReply(proxy.setConfiguration(setConf));
    } else if (request instanceof GroupListRequest){
      final GroupListRequestProto proto = ClientProtoUtils.toGroupListRequestProto(
          (GroupListRequest) request);
      return ClientProtoUtils.toGroupListReply(proxy.groupList(proto));
    } else if (request instanceof GroupInfoRequest){
      final GroupInfoRequestProto proto = ClientProtoUtils.toGroupInfoRequestProto(
          (GroupInfoRequest) request);
      return ClientProtoUtils.toGroupInfoReply(proxy.groupInfo(proto));
    } else if (request instanceof TransferLeadershipRequest) {
      final TransferLeadershipRequestProto proto = ClientProtoUtils.toTransferLeadershipRequestProto(
          (TransferLeadershipRequest) request);
      return ClientProtoUtils.toRaftClientReply(proxy.transferLeadership(proto));
    } else if (request instanceof SnapshotManagementRequest) {
      final SnapshotManagementRequestProto proto = ClientProtoUtils.toSnapshotManagementRequestProto
          ((SnapshotManagementRequest) request);
      return ClientProtoUtils.toRaftClientReply(proxy.snapshotManagement(proto));
    } else if (request instanceof LeaderElectionManagementRequest) {
      final LeaderElectionManagementRequestProto proto = ClientProtoUtils.toLeaderElectionManagementRequestProto
          ((LeaderElectionManagementRequest) request);
      return ClientProtoUtils.toRaftClientReply(proxy.leaderElectionManagement(proto));
    } else {
      final CompletableFuture<RaftClientReply> f = sendRequest(request, proxy);
      // TODO: timeout support
      // 设置超时，以避免永久阻塞
      try {
        long timeout = Math.max(request.getTimeoutMs(), 10000);
        return f.get(timeout, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new InterruptedIOException(
            "Interrupted while waiting for response of request " + request);
      } catch (ExecutionException e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(clientId + ": failed " + request, e);
        }
        throw IOUtils.toIOException(e);
      } catch (TimeoutException e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(clientId + ": timeout " + request, e);
        }
        throw IOUtils.toIOException(e);
      }
    }
  }

  private CompletableFuture<RaftClientReply> sendRequest(
      RaftClientRequest request, GrpcClientProtocolClient proxy) throws IOException {
    final RaftClientRequestProto requestProto =
        toRaftClientRequestProto(request);
    final CompletableFuture<RaftClientReplyProto> replyFuture = new CompletableFuture<>();
    // create a new grpc stream for each non-async call.
    final StreamObserver<RaftClientRequestProto> requestObserver =
        proxy.unorderedWithTimeout(new StreamObserver<RaftClientReplyProto>() {
          @Override
          public void onNext(RaftClientReplyProto value) {
            replyFuture.complete(value);
          }

          @Override
          public void onError(Throwable t) {
            replyFuture.completeExceptionally(GrpcUtil.unwrapIOException(t));
          }

          @Override
          public void onCompleted() {
            if (!replyFuture.isDone()) {
              replyFuture.completeExceptionally(
                  new AlreadyClosedException(clientId + ": Stream completed but no reply for request " + request));
            }
          }
        });
    requestObserver.onNext(requestProto);
    requestObserver.onCompleted();

    return replyFuture.thenApply(ClientProtoUtils::toRaftClientReply);
  }

  private RaftClientRequestProto toRaftClientRequestProto(RaftClientRequest request) throws IOException {
    final RaftClientRequestProto proto = ClientProtoUtils.toRaftClientRequestProto(request);
    if (proto.getSerializedSize() > maxMessageSize) {
      throw new IOException(clientId + ": Message size:" + proto.getSerializedSize()
          + " exceeds maximum:" + maxMessageSize);
    }
    return proto;
  }

  @Override
  public boolean shouldReconnect(Throwable e) {
    final Throwable cause = e.getCause();
    if (e instanceof IOException && cause instanceof StatusRuntimeException) {
      return !((StatusRuntimeException) cause).getStatus().isOk();
    } else if (e instanceof IllegalArgumentException) {
      return e.getMessage().contains("null frame before EOS");
    }
    return super.shouldReconnect(e);
  }
}
