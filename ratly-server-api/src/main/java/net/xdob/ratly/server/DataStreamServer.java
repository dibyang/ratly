
package net.xdob.ratly.server;

import java.io.Closeable;

/**
 * Interface for streaming server.
 */
public interface DataStreamServer extends Closeable {
  /**
   * Get network interface for server.
   */
  DataStreamServerRpc getServerRpc();
}
