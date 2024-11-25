
package net.xdob.ratly.grpc.server;

import net.xdob.ratly.server.RaftServerRpc;
import net.xdob.ratly.util.*;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.grpc.GrpcConfigKeys;
import net.xdob.ratly.grpc.GrpcTlsConfig;
import net.xdob.ratly.grpc.GrpcUtil;
import net.xdob.ratly.grpc.metrics.MessageMetrics;
import net.xdob.ratly.grpc.metrics.ZeroCopyMetrics;
import net.xdob.ratly.grpc.metrics.intercept.server.MetricServerInterceptor;
import net.xdob.ratly.protocol.AdminAsynchronousProtocol;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.rpc.SupportedRpcType;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.RaftServerConfigKeys;
import net.xdob.ratly.server.RaftServerRpcWithProxy;
import net.xdob.ratly.server.protocol.RaftServerAsynchronousProtocol;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContextBuilder;

import net.xdob.ratly.proto.raft.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static io.netty.handler.ssl.SslProvider.OPENSSL;

/** A grpc implementation of {@link RaftServerRpc}. */
public final class GrpcServicesImpl
    extends RaftServerRpcWithProxy<GrpcServerProtocolClient, PeerProxyMap<GrpcServerProtocolClient>>
    implements GrpcServices {
  static final Logger LOG = LoggerFactory.getLogger(GrpcServicesImpl.class);
  public static final String GRPC_SEND_SERVER_REQUEST =
      JavaUtils.getClassSimpleName(GrpcServicesImpl.class) + ".sendRequest";

  class AsyncService implements RaftServerAsynchronousProtocol {

    @Override
    public CompletableFuture<AppendEntriesReplyProto> appendEntriesAsync(AppendEntriesRequestProto request) {
      throw new UnsupportedOperationException("This method is not supported");
    }

    @Override
    public CompletableFuture<ReadIndexReplyProto> readIndexAsync(ReadIndexRequestProto request) throws IOException {
      CodeInjectionForTesting.execute(GRPC_SEND_SERVER_REQUEST, getId(), null, request);

      final CompletableFuture<ReadIndexReplyProto> f = new CompletableFuture<>();
      final StreamObserver<ReadIndexReplyProto> s = new StreamObserver<ReadIndexReplyProto>() {
        @Override
        public void onNext(ReadIndexReplyProto reply) {
          f.complete(reply);
        }

        @Override
        public void onError(Throwable throwable) {
          f.completeExceptionally(throwable);
        }

        @Override
        public void onCompleted() {
        }
      };

      final RaftPeerId target = RaftPeerId.valueOf(request.getServerRequest().getReplyId());
      getProxies().getProxy(target).readIndex(request, s);
      return f;
    }
  }

  public static final class Builder {
    private RaftServer server;
    private Customizer customizer;

    private String adminHost;
    private int adminPort;
    private GrpcTlsConfig adminTlsConfig;
    private String clientHost;
    private int clientPort;
    private GrpcTlsConfig clientTlsConfig;
    private String serverHost;
    private int serverPort;
    private GrpcTlsConfig serverTlsConfig;

    private SizeInBytes messageSizeMax;
    private SizeInBytes flowControlWindow;
    private TimeDuration requestTimeoutDuration;
    private boolean separateHeartbeatChannel;
    private boolean zeroCopyEnabled;

    private Builder() {}

    public Builder setServer(RaftServer raftServer) {
      this.server = raftServer;

      final RaftProperties properties = server.getProperties();
      this.adminHost = GrpcConfigKeys.Admin.host(properties);
      this.adminPort = GrpcConfigKeys.Admin.port(properties);
      this.clientHost = GrpcConfigKeys.Client.host(properties);
      this.clientPort = GrpcConfigKeys.Client.port(properties);
      this.serverHost = GrpcConfigKeys.Server.host(properties);
      this.serverPort = GrpcConfigKeys.Server.port(properties);
      this.messageSizeMax = GrpcConfigKeys.messageSizeMax(properties, LOG::info);
      this.flowControlWindow = GrpcConfigKeys.flowControlWindow(properties, LOG::info);
      this.requestTimeoutDuration = RaftServerConfigKeys.Rpc.requestTimeout(properties);
      this.separateHeartbeatChannel = GrpcConfigKeys.Server.heartbeatChannel(properties);
      this.zeroCopyEnabled = GrpcConfigKeys.Server.zeroCopyEnabled(properties);

      final SizeInBytes appenderBufferSize = RaftServerConfigKeys.Log.Appender.bufferByteLimit(properties);
      final SizeInBytes gap = SizeInBytes.ONE_MB;
      final long diff = messageSizeMax.getSize() - appenderBufferSize.getSize();
      if (diff < gap.getSize()) {
        throw new IllegalArgumentException("Illegal configuration: "
            + GrpcConfigKeys.MESSAGE_SIZE_MAX_KEY + "(= " + messageSizeMax
            + ") must be " + gap + " larger than "
            + RaftServerConfigKeys.Log.Appender.BUFFER_BYTE_LIMIT_KEY + "(= " + appenderBufferSize + ").");
      }

      return this;
    }

    public Builder setCustomizer(Customizer customizer) {
      this.customizer = customizer != null? customizer : Customizer.getDefaultInstance();
      return this;
    }

    private GrpcServerProtocolClient newGrpcServerProtocolClient(RaftPeer target) {
      return new GrpcServerProtocolClient(target, flowControlWindow.getSizeInt(),
          requestTimeoutDuration, serverTlsConfig, separateHeartbeatChannel);
    }

    private ExecutorService newExecutor() {
      final RaftProperties properties = server.getProperties();
      return Concurrents3.newThreadPoolWithMax(
          GrpcConfigKeys.Server.asyncRequestThreadPoolCached(properties),
          GrpcConfigKeys.Server.asyncRequestThreadPoolSize(properties),
          server.getId() + "-request-");
    }

    private GrpcClientProtocolService newGrpcClientProtocolService(
        ExecutorService executor, ZeroCopyMetrics zeroCopyMetrics) {
      return new GrpcClientProtocolService(server::getId, server, executor, zeroCopyEnabled, zeroCopyMetrics);
    }

    private GrpcServerProtocolService newGrpcServerProtocolService(ZeroCopyMetrics zeroCopyMetrics) {
      return new GrpcServerProtocolService(server::getId, server, zeroCopyEnabled, zeroCopyMetrics);
    }

    private MetricServerInterceptor newMetricServerInterceptor() {
      return new MetricServerInterceptor(server::getId,
          JavaUtils.getClassSimpleName(getClass()) + "_" + serverPort);
    }

    Server buildServer(NettyServerBuilder builder, EnumSet<Type> types) {
      return customizer.customize(builder, types).build();
    }

    private NettyServerBuilder newNettyServerBuilderForServer() {
      return newNettyServerBuilder(serverHost, serverPort, serverTlsConfig);
    }

    private NettyServerBuilder newNettyServerBuilderForAdmin() {
      return newNettyServerBuilder(adminHost, adminPort, adminTlsConfig);
    }

    private NettyServerBuilder newNettyServerBuilderForClient() {
      return newNettyServerBuilder(clientHost, clientPort, clientTlsConfig);
    }

    private NettyServerBuilder newNettyServerBuilder(String hostname, int port, GrpcTlsConfig tlsConfig) {
      final InetSocketAddress address = hostname == null || hostname.isEmpty() ?
          new InetSocketAddress(port) : new InetSocketAddress(hostname, port);
      final NettyServerBuilder nettyServerBuilder = NettyServerBuilder.forAddress(address)
          .withChildOption(ChannelOption.SO_REUSEADDR, true)
          .maxInboundMessageSize(messageSizeMax.getSizeInt())
          .flowControlWindow(flowControlWindow.getSizeInt());

      if (tlsConfig != null) {
        LOG.info("Setting TLS for {}", address);
        SslContextBuilder sslContextBuilder = GrpcUtil.initSslContextBuilderForServer(tlsConfig.getKeyManager());
        if (tlsConfig.getMtlsEnabled()) {
          sslContextBuilder.clientAuth(ClientAuth.REQUIRE);
          GrpcUtil.setTrustManager(sslContextBuilder, tlsConfig.getTrustManager());
        }
        sslContextBuilder = GrpcSslContexts.configure(sslContextBuilder, OPENSSL);
        try {
          nettyServerBuilder.sslContext(sslContextBuilder.build());
        } catch (Exception ex) {
          throw new IllegalArgumentException("Failed to build SslContext, tlsConfig=" + tlsConfig, ex);
        }
      }
      return nettyServerBuilder;
    }

    private boolean separateAdminServer() {
      return adminPort > 0 && adminPort != serverPort;
    }

    private boolean separateClientServer() {
      return clientPort > 0 && clientPort != serverPort;
    }

    Server newServer(GrpcClientProtocolService client, ZeroCopyMetrics zeroCopyMetrics, ServerInterceptor interceptor) {
      final EnumSet<Type> types = EnumSet.of(Type.SERVER);
      final NettyServerBuilder serverBuilder = newNettyServerBuilderForServer();
      final ServerServiceDefinition service = newGrpcServerProtocolService(zeroCopyMetrics).bindServiceWithZeroCopy();
      serverBuilder.addService(ServerInterceptors.intercept(service, interceptor));

      if (!separateAdminServer()) {
        types.add(Type.ADMIN);
        addAdminService(serverBuilder, server, interceptor);
      }
      if (!separateClientServer()) {
        types.add(Type.CLIENT);
        addClientService(serverBuilder, client, interceptor);
      }
      return buildServer(serverBuilder, types);
    }

    public GrpcServicesImpl build() {
      return new GrpcServicesImpl(this);
    }

    public Builder setAdminTlsConfig(GrpcTlsConfig config) {
      this.adminTlsConfig = config;
      return this;
    }

    public Builder setClientTlsConfig(GrpcTlsConfig config) {
      this.clientTlsConfig = config;
      return this;
    }

    public Builder setServerTlsConfig(GrpcTlsConfig config) {
      this.serverTlsConfig = config;
      return this;
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final Map<String, Server> servers = new HashMap<>();
  private final Supplier<InetSocketAddress> addressSupplier;
  private final Supplier<InetSocketAddress> clientServerAddressSupplier;
  private final Supplier<InetSocketAddress> adminServerAddressSupplier;

  private final AsyncService asyncService = new AsyncService();

  private final ExecutorService executor;
  private final GrpcClientProtocolService clientProtocolService;

  private final MetricServerInterceptor serverInterceptor;
  private final ZeroCopyMetrics zeroCopyMetrics = new ZeroCopyMetrics();

  private GrpcServicesImpl(Builder b) {
    super(b.server::getId, id -> new PeerProxyMap<>(id.toString(), b::newGrpcServerProtocolClient));

    this.executor = b.newExecutor();
    this.clientProtocolService = b.newGrpcClientProtocolService(executor, zeroCopyMetrics);
    this.serverInterceptor = b.newMetricServerInterceptor();
    final Server server = b.newServer(clientProtocolService, zeroCopyMetrics, serverInterceptor);

    servers.put(GrpcServerProtocolService.class.getSimpleName(), server);
    addressSupplier = newAddressSupplier(b.serverPort, server);

    if (b.separateAdminServer()) {
      final NettyServerBuilder builder = b.newNettyServerBuilderForAdmin();
      addAdminService(builder, b.server, serverInterceptor);
      final Server adminServer = b.buildServer(builder, EnumSet.of(Type.ADMIN));
      servers.put(GrpcAdminProtocolService.class.getName(), adminServer);
      adminServerAddressSupplier = newAddressSupplier(b.adminPort, adminServer);
    } else {
      adminServerAddressSupplier = addressSupplier;
    }

    if (b.separateClientServer()) {
      final NettyServerBuilder builder = b.newNettyServerBuilderForClient();
      addClientService(builder, clientProtocolService, serverInterceptor);
      final Server clientServer = b.buildServer(builder, EnumSet.of(Type.CLIENT));
      servers.put(GrpcClientProtocolService.class.getName(), clientServer);
      clientServerAddressSupplier = newAddressSupplier(b.clientPort, clientServer);
    } else {
      clientServerAddressSupplier = addressSupplier;
    }
  }

  static MemoizedSupplier<InetSocketAddress> newAddressSupplier(int port, Server server) {
    return JavaUtils.memoize(() -> new InetSocketAddress(port != 0 ? port : server.getPort()));
  }

  static void addClientService(NettyServerBuilder builder, GrpcClientProtocolService client,
      ServerInterceptor interceptor) {
    final ServerServiceDefinition service = client.bindServiceWithZeroCopy();
    builder.addService(ServerInterceptors.intercept(service, interceptor));
  }

  static void addAdminService(NettyServerBuilder builder, AdminAsynchronousProtocol admin,
      ServerInterceptor interceptor) {
    final GrpcAdminProtocolService service = new GrpcAdminProtocolService(admin);
    builder.addService(ServerInterceptors.intercept(service, interceptor));
  }

  @Override
  public SupportedRpcType getRpcType() {
    return SupportedRpcType.GRPC;
  }

  @Override
  public void startImpl() {
    for (Server server : servers.values()) {
      try {
        server.start();
      } catch (IOException e) {
        ExitUtils.terminate(1, "Failed to start Grpc server", e, LOG);
      }
      LOG.info("{}: {} started, listening on {}",
          getId(), JavaUtils.getClassSimpleName(getClass()), server.getPort());
    }
  }

  @Override
  public void closeImpl() throws IOException {
    for (Map.Entry<String, Server> server : servers.entrySet()) {
      final String name = getId() + ": shutdown server " + server.getKey();
      LOG.info("{} now", name);
      final Server s = server.getValue().shutdownNow();
      super.closeImpl();
      try {
        s.awaitTermination();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw IOUtils.toInterruptedIOException(name + " failed", e);
      }
      LOG.info("{} successfully", name);
    }

    serverInterceptor.close();
    Concurrents3.shutdownAndWait(executor);
    zeroCopyMetrics.unregister();
  }

  @Override
  public void notifyNotLeader(RaftGroupId groupId) {
    clientProtocolService.closeAllOrderedRequestStreamObservers(groupId);
  }

  @Override
  public InetSocketAddress getInetSocketAddress() {
    return addressSupplier.get();
  }

  @Override
  public InetSocketAddress getClientServerAddress() {
    return clientServerAddressSupplier.get();
  }

  @Override
  public InetSocketAddress getAdminServerAddress() {
    return adminServerAddressSupplier.get();
  }

  @Override
  public RaftServerAsynchronousProtocol async() {
    return asyncService;
  }

  @Override
  public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto request) {
    throw new UnsupportedOperationException(
        "Blocking " + JavaUtils.getCurrentStackTraceElement().getMethodName() + " call is not supported");
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(InstallSnapshotRequestProto request) {
    throw new UnsupportedOperationException(
        "Blocking " + JavaUtils.getCurrentStackTraceElement().getMethodName() + " call is not supported");
  }

  @Override
  public RequestVoteReplyProto requestVote(RequestVoteRequestProto request)
      throws IOException {
    CodeInjectionForTesting.execute(GRPC_SEND_SERVER_REQUEST, getId(),
        null, request);

    final RaftPeerId target = RaftPeerId.valueOf(request.getServerRequest().getReplyId());
    return getProxies().getProxy(target).requestVote(request);
  }

  @Override
  public StartLeaderElectionReplyProto startLeaderElection(StartLeaderElectionRequestProto request) throws IOException {
    CodeInjectionForTesting.execute(GRPC_SEND_SERVER_REQUEST, getId(), null, request);

    final RaftPeerId target = RaftPeerId.valueOf(request.getServerRequest().getReplyId());
    return getProxies().getProxy(target).startLeaderElection(request);
  }

  @VisibleForTesting
  MessageMetrics getMessageMetrics() {
    return serverInterceptor.getMetrics();
  }

  @VisibleForTesting
  public ZeroCopyMetrics getZeroCopyMetrics() {
    return zeroCopyMetrics;
  }
}
