package net.xdob.ratly.rpc;

import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.ReflectionUtils;

/** The type of RPC implementations. */
public interface RpcType {
  /**
   * Parse the given string as a {@link SupportedRpcType}
   * or a user-defined {@link RpcType}.
   *
   * @param rpcType The string representation of an {@link RpcType}.
   * @return a {@link SupportedRpcType} or a user-defined {@link RpcType}.
   */
  static RpcType valueOf(String rpcType) {
    final Throwable fromSupportedRpcType;
    try { // Try parsing it as a SupportedRpcType
      return SupportedRpcType.valueOfIgnoreCase(rpcType);
    } catch (Exception e) {
      fromSupportedRpcType = e;
    }

    try {
      // Try using it as a class name
      return ReflectionUtils.newInstance(
          ReflectionUtils.getClass(rpcType, RpcType.class));
    } catch(Exception e) {
      final String classname = JavaUtils.getClassSimpleName(RpcType.class);
      final IllegalArgumentException iae = new IllegalArgumentException(
          "Invalid " + classname + ": \"" + rpcType + "\" "
              + " cannot be used as a user-defined " + classname
              + " and it is not a " + JavaUtils.getClassSimpleName(SupportedRpcType.class) + ".");
      iae.addSuppressed(e);
      iae.addSuppressed(fromSupportedRpcType);
      throw iae;
    }
  }

  /** @return the name of the rpc type. */
  String name();

  /** @return a new factory created using the given parameters. */
  RpcFactory newFactory(Parameters parameters);

  /**
   * 获取 RPC 类型 {@link RpcType}的接口。
   */
  interface Get {
    /** @return the {@link RpcType}. */
    RpcType getRpcType();
  }
}