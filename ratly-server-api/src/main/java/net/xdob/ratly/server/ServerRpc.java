
package net.xdob.ratly.server;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * A general server interface.
 */
public interface ServerRpc extends Closeable {
  /** Start the RPC service. */
  void start() throws IOException;

  /** @return the address where this RPC server is listening to. */
  InetSocketAddress getInetSocketAddress();
}
