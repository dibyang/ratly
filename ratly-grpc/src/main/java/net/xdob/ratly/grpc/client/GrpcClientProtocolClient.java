
package net.xdob.ratly.grpc.client;

import net.xdob.ratly.client.RaftClientConfigKeys;
import net.xdob.ratly.client.impl.ClientProtoUtils;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.grpc.GrpcConfigKeys;
import net.xdob.ratly.grpc.GrpcTlsConfig;
import net.xdob.ratly.grpc.GrpcUtil;
import net.xdob.ratly.grpc.metrics.intercept.client.MetricClientInterceptor;
import net.xdob.ratly.proto.raft.*;
import net.xdob.ratly.proto.grpc.AdminProtocolServiceGrpc;
import net.xdob.ratly.proto.grpc.AdminProtocolServiceGrpc.AdminProtocolServiceBlockingStub;
import net.xdob.ratly.proto.grpc.RaftClientProtocolServiceGrpc;
import net.xdob.ratly.proto.grpc.RaftClientProtocolServiceGrpc.RaftClientProtocolServiceStub;
import net.xdob.ratly.protocol.ClientId;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftClientRequest;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.exceptions.AlreadyClosedException;
import net.xdob.ratly.protocol.exceptions.LeaderNotReadyException;
import net.xdob.ratly.protocol.exceptions.NotLeaderException;
import net.xdob.ratly.protocol.exceptions.TimeoutIOException;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.handler.ssl.SslContextBuilder;
import net.xdob.ratly.util.*;
import net.xdob.ratly.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class GrpcClientProtocolClient implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(GrpcClientProtocolClient.class);

  private final Supplier<String> name;
  private final RaftPeer target;
  private final ManagedChannel clientChannel;
  private final ManagedChannel adminChannel;

  private final TimeDuration requestTimeoutDuration;
  private final TimeDuration watchRequestTimeoutDuration;
  private final TimeoutExecutor scheduler = TimeoutExecutor.getInstance();

  private final RaftClientProtocolServiceStub asyncStub;
  private final AdminProtocolServiceBlockingStub adminBlockingStub;

  private final AtomicReference<AsyncStreamObservers> orderedStreamObservers = new AtomicReference<>();

  private final AtomicReference<AsyncStreamObservers> unorderedStreamObservers = new AtomicReference<>();
  private final MetricClientInterceptor metricClientInterceptor;
	private final RaftProperties properties;

	GrpcClientProtocolClient(ClientId id, RaftPeer target, RaftProperties properties,
      GrpcTlsConfig adminTlsConfig, GrpcTlsConfig clientTlsConfig) {
    this.name = JavaUtils.memoize(() -> id + "->" + target.getId());
    this.target = target;
		this.properties = properties;
    final SizeInBytes flowControlWindow = GrpcConfigKeys.flowControlWindow(properties, LOG::debug);
    final SizeInBytes maxMessageSize = GrpcConfigKeys.messageSizeMax(properties, LOG::debug);
    metricClientInterceptor = new MetricClientInterceptor(getName());

		final String clientAddress = Optional.ofNullable(target.getClientAddress())
        .filter(x -> !x.isEmpty()).orElse(target.getAddress());
    final String adminAddress = Optional.ofNullable(target.getAdminAddress())
        .filter(x -> !x.isEmpty()).orElse(target.getAddress());
    final boolean separateAdminChannel = !Objects.equals(clientAddress, adminAddress);

    clientChannel = buildChannel(clientAddress, clientTlsConfig,
        flowControlWindow, maxMessageSize);
    adminChannel = separateAdminChannel
        ? buildChannel(adminAddress, adminTlsConfig,
            flowControlWindow, maxMessageSize)
        : clientChannel;

    asyncStub = RaftClientProtocolServiceGrpc.newStub(clientChannel);
    adminBlockingStub = AdminProtocolServiceGrpc.newBlockingStub(adminChannel);
    this.requestTimeoutDuration = RaftClientConfigKeys.Rpc.requestTimeout(properties);
    this.watchRequestTimeoutDuration =
        RaftClientConfigKeys.Rpc.watchRequestTimeout(properties);
  }

  private ManagedChannel buildChannel(String address, GrpcTlsConfig tlsConf,
      SizeInBytes flowControlWindow, SizeInBytes maxMessageSize) {
    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forTarget(address);
    // ignore any http proxy for grpc
    channelBuilder.proxyDetector(uri -> null);

    if (tlsConf != null) {
      LOG.debug("Setting TLS for {}", address);
      SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();
      GrpcUtil.setTrustManager(sslContextBuilder, tlsConf.getTrustManager());
      if (tlsConf.getMtlsEnabled()) {
        GrpcUtil.setKeyManager(sslContextBuilder, tlsConf.getKeyManager());
      }
      try {
        channelBuilder.useTransportSecurity().sslContext(
            sslContextBuilder.build());
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    } else {
      channelBuilder.negotiationType(NegotiationType.PLAINTEXT);
    }

		return channelBuilder.flowControlWindow(flowControlWindow.getSizeInt())
        .maxInboundMessageSize(maxMessageSize.getSizeInt())
        .intercept(metricClientInterceptor)
        .build();
  }

  String getName() {
    return name.get();
  }

  @Override
  public void close() {
    Optional.ofNullable(orderedStreamObservers.getAndSet(null)).ifPresent(AsyncStreamObservers::close);
    Optional.ofNullable(unorderedStreamObservers.getAndSet(null)).ifPresent(AsyncStreamObservers::close);
    GrpcUtil.shutdownManagedChannel(clientChannel);
    if (clientChannel != adminChannel) {
      GrpcUtil.shutdownManagedChannel(adminChannel);
    }
    metricClientInterceptor.close();
  }

  RaftClientReplyProto groupAdd(GroupManagementRequestProto request) throws IOException {
    return blockingCall(() -> adminBlockingStub
        .withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
        .groupManagement(request));
  }

  GroupListReplyProto groupList(GroupListRequestProto request) {
    return adminBlockingStub
        .withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
        .groupList(request);
  }

  GroupInfoReplyProto groupInfo(GroupInfoRequestProto request) {
    return adminBlockingStub
        .withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
        .groupInfo(request);
  }

  DRpcReplyProto invokeRpc(DRpcRequestProto request) {
    return adminBlockingStub
        .withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
        .invokeRpc(request);
  }

  RaftClientReplyProto setConfiguration(
      SetConfigurationRequestProto request) throws IOException {
    return blockingCall(() -> adminBlockingStub
        .withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
        .setConfiguration(request));
  }

  RaftClientReplyProto transferLeadership(
      TransferLeadershipRequestProto request) throws IOException {
    TimeDuration newDuration = requestTimeoutDuration.add(
        request.getRpcRequest().getTimeoutMs(), TimeUnit.MILLISECONDS);
    return blockingCall(() -> adminBlockingStub
        .withDeadlineAfter(newDuration.getDuration(), newDuration.getUnit())
        .transferLeadership(request));
  }

  RaftClientReplyProto snapshotManagement(
      SnapshotManagementRequestProto request) throws IOException {
    return blockingCall(() -> adminBlockingStub
        .withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
        .snapshotManagement(request));
  }

  RaftClientReplyProto leaderElectionManagement(
      LeaderElectionManagementRequestProto request) throws IOException {
    return blockingCall(() -> adminBlockingStub
        .withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
        .leaderElectionManagement(request));
  }

  private static RaftClientReplyProto blockingCall(
      CheckedSupplier<RaftClientReplyProto, StatusRuntimeException> supplier
      ) throws IOException {
    try {
      return supplier.get();
    } catch (StatusRuntimeException e) {
      throw GrpcUtil.unwrapException(e);
    }
  }

  StreamObserver<RaftClientRequestProto> ordered(StreamObserver<RaftClientReplyProto> responseHandler) {
    return asyncStub.ordered(responseHandler);
  }

  StreamObserver<RaftClientRequestProto> unorderedWithTimeout(StreamObserver<RaftClientReplyProto> responseHandler) {
    return asyncStub.withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
        .unordered(responseHandler);
  }

  AsyncStreamObservers getOrderedStreamObservers() {
    return orderedStreamObservers.updateAndGet(
        a -> a != null? a : new AsyncStreamObservers(this::ordered));
  }

  AsyncStreamObservers getUnorderedAsyncStreamObservers() {
    return unorderedStreamObservers.updateAndGet(
        a -> a != null? a : new AsyncStreamObservers(asyncStub::unordered));
  }

  public RaftPeer getTarget() {
    return target;
  }

  class ReplyMap {
    private final AtomicReference<Map<Long, CompletableFuture<RaftClientReply>>> map
        = new AtomicReference<>(new ConcurrentHashMap<>());

    // synchronized to avoid putNew after getAndSetNull
    synchronized CompletableFuture<RaftClientReply> putNew(long callId) {
      return Optional.ofNullable(map.get())
          .map(m -> Collections3.putNew(callId, new CompletableFuture<>(), m, this::toString))
          .orElse(null);
    }

    Optional<CompletableFuture<RaftClientReply>> remove(long callId) {
      return Optional.ofNullable(map.get()).map(m -> m.remove(callId));
    }

    // synchronized to avoid putNew after getAndSetNull
    synchronized Map<Long, CompletableFuture<RaftClientReply>> getAndSetNull() {
      return map.getAndSet(null);
    }

    @Override
    public String toString() {
      return getName() + ":" + JavaUtils.getClassSimpleName(getClass());
    }
  }

  static class RequestStreamer {
    private final AtomicReference<StreamObserver<RaftClientRequestProto>> streamObserver;

    RequestStreamer(StreamObserver<RaftClientRequestProto> streamObserver) {
      this.streamObserver = new AtomicReference<>(streamObserver);
    }

    synchronized boolean onNext(RaftClientRequestProto request) {
      final StreamObserver<RaftClientRequestProto> s = streamObserver.get();
      if (s != null) {
        s.onNext(request);
        return true;
      }
      return false;
    }

    synchronized void onCompleted() {
      final StreamObserver<RaftClientRequestProto> s = streamObserver.getAndSet(null);
      if (s != null) {
        s.onCompleted();
      }
    }
  }

  class AsyncStreamObservers {
    /** Request map: callId -> future */
    private final ReplyMap replies = new ReplyMap();

    private final StreamObserver<RaftClientReplyProto> replyStreamObserver
        = new StreamObserver<RaftClientReplyProto>() {
      @Override
      public void onNext(RaftClientReplyProto proto) {
        final long callId = proto.getRpcReply().getCallId();
        try {
          final RaftClientReply reply = ClientProtoUtils.toRaftClientReply(proto);
          LOG.trace("{}: receive {}", getName(), reply);
          final NotLeaderException nle = reply.getNotLeaderException();
          if (nle != null) {
            completeReplyExceptionally(nle, NotLeaderException.class.getName());
            return;
          }
          final LeaderNotReadyException lnre = reply.getLeaderNotReadyException();
          if (lnre != null) {
            completeReplyExceptionally(lnre, LeaderNotReadyException.class.getName());
            return;
          }
          handleReplyFuture(callId, f -> f.complete(reply));
        } catch (Exception e) {
          handleReplyFuture(callId, f -> f.completeExceptionally(e));
        }
      }

      @Override
      public void onError(Throwable t) {
        final IOException ioe = GrpcUtil.unwrapIOException(t);
        completeReplyExceptionally(ioe, "onError");
      }

      @Override
      public void onCompleted() {
        completeReplyExceptionally(null, "completed");
      }
    };
    private final RequestStreamer requestStreamer;

    AsyncStreamObservers(Function<StreamObserver<RaftClientReplyProto>, StreamObserver<RaftClientRequestProto>> f) {
      this.requestStreamer = new RequestStreamer(f.apply(replyStreamObserver));
    }

    CompletableFuture<RaftClientReply> onNext(RaftClientRequest request) {
      final long callId = request.getCallId();
      final CompletableFuture<RaftClientReply> f = replies.putNew(callId);
      if (f == null) {
        return JavaUtils.completeExceptionally(new AlreadyClosedException(getName() + " is closed."));
      }
      try {
        if (!requestStreamer.onNext(ClientProtoUtils.toRaftClientRequestProto(request))) {
          return JavaUtils.completeExceptionally(new AlreadyClosedException(getName() + ": the stream is closed."));
        }
      } catch(Exception t) {
        handleReplyFuture(request.getCallId(), future -> future.completeExceptionally(t));
        return f;
      }

      if (RaftClientRequestProto.TypeCase.WATCH.equals(request.getType().getTypeCase())) {
        scheduler.onTimeout(watchRequestTimeoutDuration, () ->
                timeoutCheck(callId, watchRequestTimeoutDuration), LOG,
            () -> "Timeout check failed for client request #" + callId);
      } else {
        scheduler.onTimeout(requestTimeoutDuration,
            () -> timeoutCheck(callId, requestTimeoutDuration), LOG,
            () -> "Timeout check failed for client request #" + callId);
      }
      return f;
    }

    private void timeoutCheck(long callId, TimeDuration timeOutDuration) {
      handleReplyFuture(callId, f -> f.completeExceptionally(
          new TimeoutIOException(getName() + " request #" + callId + " timeout " + timeOutDuration)));
    }

    private void handleReplyFuture(long callId, Consumer<CompletableFuture<RaftClientReply>> handler) {
      replies.remove(callId).ifPresent(handler);
    }

    private void close() {
      requestStreamer.onCompleted();
      completeReplyExceptionally(null, "close");
    }

    private void completeReplyExceptionally(Throwable t, String event) {
      final Map<Long, CompletableFuture<RaftClientReply>> map = replies.getAndSetNull();
      if (map == null) {
        return;
      }
      for (Map.Entry<Long, CompletableFuture<RaftClientReply>> entry : map.entrySet()) {
        final CompletableFuture<RaftClientReply> f = entry.getValue();
        if (!f.isDone()) {
          f.completeExceptionally(t != null? t
              : new AlreadyClosedException(getName() + ": Stream " + event
                  + ": no reply for async request cid=" + entry.getKey()));
        }
      }
    }
  }
}
