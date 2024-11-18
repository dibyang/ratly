
package net.xdob.ratly.grpc.server;

import net.xdob.ratly.server.RaftServerRpc;
import io.grpc.netty.NettyServerBuilder;

import java.util.EnumSet;

/** The gRPC services extending {@link RaftServerRpc}. */
public interface GrpcServices extends RaftServerRpc {
  /** The type of the services. */
  enum Type {ADMIN, CLIENT, SERVER}

  /**
   * To customize the services.
   * For example, add a custom service.
   */
  interface Customizer {
    /** The default NOOP {@link Customizer}. */
    class Default implements Customizer {
      private static final Default INSTANCE = new Default();

      @Override
      public NettyServerBuilder customize(NettyServerBuilder builder, EnumSet<Type> types) {
        return builder;
      }
    }

    static Customizer getDefaultInstance() {
      return Default.INSTANCE;
    }

    /**
     * Customize the given builder for the given types.
     *
     * @return the customized builder.
     */
    NettyServerBuilder customize(NettyServerBuilder builder, EnumSet<Type> types);
  }
}
