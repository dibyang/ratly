package net.xdob.ratly.rpc;

import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.util.ReflectionUtils;

/** The RPC types supported. */
public enum SupportedRpcType implements RpcType {
  NETTY("net.xdob.ratly.netty.NettyFactory"),
  GRPC("net.xdob.ratly.grpc.GrpcFactory"),
  HADOOP("net.xdob.ratly.hadooprpc.HadoopFactory");

  /** Same as {@link #valueOf(String)} except that this method is case insensitive. */
  public static SupportedRpcType valueOfIgnoreCase(String s) {
    return valueOf(s.toUpperCase());
  }

  private static final Class<?>[] ARG_CLASSES = {Parameters.class};

  private final String factoryClassName;

  SupportedRpcType(String factoryClassName) {
    this.factoryClassName = factoryClassName;
  }

  @Override
  public RpcFactory newFactory(Parameters parameters) {
    final Class<? extends RpcFactory> clazz = ReflectionUtils.getClass(
        factoryClassName, RpcFactory.class);
    return ReflectionUtils.newInstance(clazz, ARG_CLASSES, parameters);
  }
}
