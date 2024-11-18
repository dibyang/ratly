
package net.xdob.ratly.grpc;

import net.xdob.ratly.client.ClientFactory;
import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.grpc.client.GrpcClientRpc;
import net.xdob.ratly.grpc.server.GrpcLogAppender;
import net.xdob.ratly.grpc.server.GrpcServices;
import net.xdob.ratly.grpc.server.GrpcServicesImpl;
import net.xdob.ratly.protocol.ClientId;
import net.xdob.ratly.rpc.SupportedRpcType;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.leader.LogAppender;
import net.xdob.ratly.server.ServerFactory;
import net.xdob.ratly.server.leader.FollowerInfo;
import net.xdob.ratly.server.leader.LeaderState;
import io.netty.buffer.PooledByteBufAllocator;
import net.xdob.ratly.util.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class GrpcFactory implements ServerFactory, ClientFactory {

  public static final Logger LOG = LoggerFactory.getLogger(GrpcFactory.class);

  static final String USE_CACHE_FOR_ALL_THREADS_NAME = "useCacheForAllThreads";
  static final String USE_CACHE_FOR_ALL_THREADS_KEY = "io.netty.allocator."
      + USE_CACHE_FOR_ALL_THREADS_NAME;
  static {
    // see io.netty.buffer.PooledByteBufAllocator.DEFAULT_USE_CACHE_FOR_ALL_THREADS
    final String value = JavaUtils.getSystemProperty(USE_CACHE_FOR_ALL_THREADS_KEY);
    if (value == null) {
      // Set the property to false, when it is not set.
      JavaUtils.setSystemProperty(USE_CACHE_FOR_ALL_THREADS_KEY, Boolean.FALSE.toString());
    }
  }

  static boolean checkPooledByteBufAllocatorUseCacheForAllThreads(Consumer<String> log) {
    final boolean value = PooledByteBufAllocator.defaultUseCacheForAllThreads();
    if (value) {
      log.accept("PERFORMANCE WARNING: " + USE_CACHE_FOR_ALL_THREADS_NAME + " is " + true
          + " that may cause Netty to create a lot garbage objects and, as a result, trigger GC.\n"
          + "\tIt is recommended to disable " + USE_CACHE_FOR_ALL_THREADS_NAME
          + " by setting -D" + USE_CACHE_FOR_ALL_THREADS_KEY + "=" + false + " in command line.");
    }
    return value;
  }

  private final GrpcServices.Customizer servicesCustomizer;

  private final GrpcTlsConfig tlsConfig;
  private final GrpcTlsConfig adminTlsConfig;
  private final GrpcTlsConfig clientTlsConfig;
  private final GrpcTlsConfig serverTlsConfig;

  public static Parameters newRaftParameters(GrpcTlsConfig conf) {
    final Parameters p = new Parameters();
    GrpcConfigKeys.TLS.setConf(p, conf);
    return p;
  }

  public GrpcFactory(Parameters parameters) {
    this(GrpcConfigKeys.Server.servicesCustomizer(parameters),
        GrpcConfigKeys.TLS.conf(parameters),
        GrpcConfigKeys.Admin.tlsConf(parameters),
        GrpcConfigKeys.Client.tlsConf(parameters),
        GrpcConfigKeys.Server.tlsConf(parameters)
    );
  }

  public GrpcFactory(GrpcTlsConfig tlsConfig) {
    this(null, tlsConfig, null, null, null);
  }

  private GrpcFactory(GrpcServices.Customizer servicesCustomizer,
      GrpcTlsConfig tlsConfig, GrpcTlsConfig adminTlsConfig,
      GrpcTlsConfig clientTlsConfig, GrpcTlsConfig serverTlsConfig) {
    this.servicesCustomizer = servicesCustomizer;

    this.tlsConfig = tlsConfig;
    this.adminTlsConfig = adminTlsConfig;
    this.clientTlsConfig = clientTlsConfig;
    this.serverTlsConfig = serverTlsConfig;
  }

  public GrpcTlsConfig getTlsConfig() {
    return tlsConfig;
  }

  public GrpcTlsConfig getAdminTlsConfig() {
    return adminTlsConfig != null ? adminTlsConfig : tlsConfig;
  }

  public GrpcTlsConfig getClientTlsConfig() {
    return clientTlsConfig != null ? clientTlsConfig : tlsConfig;
  }

  public GrpcTlsConfig getServerTlsConfig() {
    return serverTlsConfig != null ? serverTlsConfig : tlsConfig;
  }

  @Override
  public SupportedRpcType getRpcType() {
    return SupportedRpcType.GRPC;
  }

  @Override
  public LogAppender newLogAppender(RaftServer.Division server, LeaderState state, FollowerInfo f) {
    return new GrpcLogAppender(server, state, f);
  }

  @Override
  public GrpcServices newRaftServerRpc(RaftServer server) {
    checkPooledByteBufAllocatorUseCacheForAllThreads(LOG::info);
    return GrpcServicesImpl.newBuilder()
        .setServer(server)
        .setCustomizer(servicesCustomizer)
        .setAdminTlsConfig(getAdminTlsConfig())
        .setServerTlsConfig(getServerTlsConfig())
        .setClientTlsConfig(getClientTlsConfig())
        .build();
  }

  @Override
  public GrpcClientRpc newRaftClientRpc(ClientId clientId, RaftProperties properties) {
    checkPooledByteBufAllocatorUseCacheForAllThreads(LOG::debug);
    return new GrpcClientRpc(clientId, properties,
        getAdminTlsConfig(), getClientTlsConfig());
  }
}
