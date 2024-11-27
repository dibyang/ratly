
package net.xdob.ratly.server;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * RPC 服务器的基本接口，包含处理 RPC 请求的方法。
 */
public interface ServerRpc extends Closeable {
  /** Start the RPC service. */
  void start() throws IOException;

  /** @return the address where this RPC server is listening to. */
  InetSocketAddress getInetSocketAddress();
}
