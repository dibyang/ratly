
package net.xdob.ratly.server;

import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.rpc.RpcType;
import net.xdob.ratly.server.protocol.RaftServerAsynchronousProtocol;
import net.xdob.ratly.server.protocol.RaftServerProtocol;
import net.xdob.ratly.util.JavaUtils;

import java.io.Closeable;
import java.net.InetSocketAddress;

/**
 * An server-side interface for supporting different RPC implementations
 * such as Netty, gRPC and Hadoop.
 */
public interface RaftServerRpc extends RaftServerProtocol, ServerRpc, RpcType.Get, RaftPeer.Add, Closeable {
  /** @return the address where this RPC server is listening for client requests */
  default InetSocketAddress getClientServerAddress() {
    return getInetSocketAddress();
  }
  /** @return the address where this RPC server is listening for admin requests */
  default InetSocketAddress getAdminServerAddress() {
    return getInetSocketAddress();
  }

  /** Handle the given exception.  For example, try reconnecting. */
  void handleException(RaftPeerId serverId, Exception e, boolean reconnect);

  /** The server role changes from leader to a non-leader role. */
  default void notifyNotLeader(RaftGroupId groupId) {
  }

  default RaftServerAsynchronousProtocol async() {
    throw new UnsupportedOperationException(getClass().getName()
        + " does not support " + JavaUtils.getClassSimpleName(RaftServerAsynchronousProtocol.class));
  }
}
