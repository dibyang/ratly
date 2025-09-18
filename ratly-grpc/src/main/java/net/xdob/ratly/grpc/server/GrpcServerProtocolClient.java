package net.xdob.ratly.grpc.server;

import net.xdob.ratly.grpc.GrpcTlsConfig;
import net.xdob.ratly.grpc.GrpcUtil;
import net.xdob.ratly.grpc.util.StreamObserverWithTimeout;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.util.ServerStringUtils;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import net.xdob.ratly.proto.raft.*;
import net.xdob.ratly.proto.grpc.RaftServerProtocolServiceGrpc;
import net.xdob.ratly.proto.grpc.RaftServerProtocolServiceGrpc.RaftServerProtocolServiceBlockingStub;
import net.xdob.ratly.proto.grpc.RaftServerProtocolServiceGrpc.RaftServerProtocolServiceStub;
import net.xdob.ratly.protocol.RaftPeer;
import io.netty.handler.ssl.SslContextBuilder;
import net.xdob.ratly.util.Concurrents3;
import net.xdob.ratly.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;

/**
 * 这是一个支持向 raft 环流式传输数据的 RaftClient 实现。
 * 流式传输的实现使用了 gRPC。
 */
public class GrpcServerProtocolClient implements Closeable {
  // Common channel
  private final ManagedChannel channel;
  // Channel and stub for heartbeat
  private ManagedChannel hbChannel;
  private RaftServerProtocolServiceStub hbAsyncStub;
  private final RaftServerProtocolServiceStub asyncStub;
  private final RaftServerProtocolServiceBlockingStub blockingStub;
  private final boolean useSeparateHBChannel;

  private final TimeDuration requestTimeoutDuration;
  private static final Logger LOG = LoggerFactory.getLogger(GrpcServerProtocolClient.class);
  //visible for using in log / error messages AND to use in instrumented tests
  private final RaftPeerId raftPeerId;

  public GrpcServerProtocolClient(RaftPeer target, int flowControlWindow,
      TimeDuration requestTimeout, GrpcTlsConfig tlsConfig, boolean separateHBChannel) {
    raftPeerId = target.getId();
    LOG.info("Build channel for {}", target);
    useSeparateHBChannel = separateHBChannel;
    channel = buildChannel(target, flowControlWindow, tlsConfig);
    blockingStub = RaftServerProtocolServiceGrpc.newBlockingStub(channel);
    asyncStub = RaftServerProtocolServiceGrpc.newStub(channel);
    if (useSeparateHBChannel) {
      hbChannel = buildChannel(target, flowControlWindow, tlsConfig);
      hbAsyncStub = RaftServerProtocolServiceGrpc.newStub(hbChannel);
    }
    requestTimeoutDuration = requestTimeout;
  }

  private ManagedChannel buildChannel(RaftPeer target, int flowControlWindow,
      GrpcTlsConfig tlsConfig) {
    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forTarget(target.getAddress());
    // ignore any http proxy for grpc
    channelBuilder.proxyDetector(uri -> null);

    if (tlsConfig!= null) {
      SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();
      GrpcUtil.setTrustManager(sslContextBuilder, tlsConfig.getTrustManager());
      if (tlsConfig.getMtlsEnabled()) {
        GrpcUtil.setKeyManager(sslContextBuilder, tlsConfig.getKeyManager());
      }
      try {
        channelBuilder.useTransportSecurity().sslContext(sslContextBuilder.build());
      } catch (Exception ex) {
        throw new IllegalArgumentException("Failed to build SslContext, peerId=" + raftPeerId
            + ", tlsConfig=" + tlsConfig, ex);
      }
    } else {
      channelBuilder.negotiationType(NegotiationType.PLAINTEXT);
    }
    channelBuilder.disableRetry();

		return channelBuilder.flowControlWindow(flowControlWindow).build();
  }

  @Override
  public void close() {
    LOG.info("{} Close channels", raftPeerId);
    if (useSeparateHBChannel) {
      GrpcUtil.shutdownManagedChannel(hbChannel);
    }
    GrpcUtil.shutdownManagedChannel(channel);
  }

  public RequestVoteReplyProto requestVote(RequestVoteRequestProto request) {
    // the StatusRuntimeException will be handled by the caller
    RequestVoteReplyProto r =
        blockingStub.withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
            .requestVote(request);
    return r;
  }

  public StartLeaderElectionReplyProto startLeaderElection(StartLeaderElectionRequestProto request) {
    StartLeaderElectionReplyProto r =
        blockingStub.withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
            .startLeaderElection(request);
    return r;
  }

  void readIndex(ReadIndexRequestProto request, StreamObserver<ReadIndexReplyProto> s) {
    asyncStub.withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
        .readIndex(request, s);
  }

  CallStreamObserver<AppendEntriesRequestProto> appendEntries(
      StreamObserver<AppendEntriesReplyProto> responseHandler, boolean isHeartbeat) {
    if (isHeartbeat && useSeparateHBChannel) {
      return (CallStreamObserver<AppendEntriesRequestProto>) hbAsyncStub.appendEntries(responseHandler);
    } else {
      return (CallStreamObserver<AppendEntriesRequestProto>) asyncStub.appendEntries(responseHandler);
    }
  }

  StreamObserver<InstallSnapshotRequestProto> installSnapshot(
      String name, TimeDuration timeout, int limit, StreamObserver<InstallSnapshotReplyProto> responseHandler) {
    return StreamObserverWithTimeout.newInstance(name, ServerStringUtils::toInstallSnapshotRequestString,
        () -> timeout, limit, i -> asyncStub.withInterceptors(i).installSnapshot(responseHandler));
  }

  // short-circuit the backoff timer and make them reconnect immediately.
  public void resetConnectBackoff() {
    if (useSeparateHBChannel) {
      hbChannel.resetConnectBackoff();
    }
    channel.resetConnectBackoff();
  }
}
