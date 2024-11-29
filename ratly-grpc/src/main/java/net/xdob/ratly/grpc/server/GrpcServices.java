package net.xdob.ratly.grpc.server;

import net.xdob.ratly.server.RaftServerRpc;
import io.grpc.netty.NettyServerBuilder;

import java.util.EnumSet;

/**
 * 扩展了{@link RaftServerRpc}接口，专注于 gRPC 实现。
 */
public interface GrpcServices extends RaftServerRpc {
  /**
   * 服务的类型
   */
  enum Type {
    /**
     * 管理服务，例如集群管理操作。
     */
    ADMIN,
    /**
     * 客户端服务，例如处理客户端请求。
     */
    CLIENT,
    /**
     * 服务器间服务，例如心跳和数据同步。
     */
    SERVER
  }

  /**
   * 提供了对 gRPC 服务进行定制化的能力，例如添加自定义服务或调整 gRPC 配置。
   * 用户可以通过实现 Customizer 接口，自定义 gRPC 服务行为，例如：
   *    1.增加自定义的 gRPC 服务。
   *    2.配置特定的 Netty 参数（如线程池大小、TLS 设置）。
   *    3.根据服务类型调整行为。
   */
  interface Customizer {
    /**
     * 实现了 {@link Customizer} 接口，定义了一个 Default 类，内部逻辑是 NOOP（不进行任何操作）。
     * 目的是提供一个默认实现，避免调用方显式处理未定制场景。
     */
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
     * 核心方法，用于定制 gRPC 服务。
     *
     * @return the customized builder.
     */
    NettyServerBuilder customize(NettyServerBuilder builder, EnumSet<Type> types);
  }
}
