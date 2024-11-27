
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
 * 提供了 Raft 协议服务端与客户端之间的 RPC 通信支持。
 * 它继承了多个接口，以适应不同的 RPC 实现，如 Netty、gRPC。
 */
public interface RaftServerRpc extends RaftServerProtocol, ServerRpc, RpcType.Get, RaftPeer.Add, Closeable {
  /**
   * 描述：返回当前 RPC 服务器监听客户端请求的地址。
   * 默认实现：调用 getInetSocketAddress() 方法获取地址。
   * 用途：通常用于获取该服务器用于与客户端通信的网络地址。不同的 RPC 实现可能会有不同的实现细节。
   * @return the address where this RPC server is listening for client requests
   */
  default InetSocketAddress getClientServerAddress() {
    return getInetSocketAddress();
  }
  /**
   * 描述：返回当前 RPC 服务器监听管理员请求的地址。
   * 默认实现：同样调用 getInetSocketAddress() 获取地址。
   * <p>
   * 用途：这个方法提供了管理请求的网络地址，通常用于集群的管理和监控。
   * @return the address where this RPC server is listening for admin requests
   */
  default InetSocketAddress getAdminServerAddress() {
    return getInetSocketAddress();
  }

  /**
   * 用于处理异常，例如在遇到连接问题时尝试重新连接。
   * <p>
   * 用途：当服务器遇到异常（例如网络中断）时，需要根据业务需求采取相应的措施。此方法允许处理这些异常，并根据需要执行重连等操作。
   */
  void handleException(RaftPeerId serverId, Exception e, boolean reconnect);

  /**
   * 描述：当服务器的角色从领导者（Leader）变为非领导者时，调用此方法。
   * <p>
   * 用途：Raft 协议中的领导者节点在集群中的状态是动态变化的。如果一个节点不再是领导者，它需要通知其他组件，
   * 或者执行一些清理操作。此方法允许服务器在角色变化时执行相应的处理。
   */
  default void notifyNotLeader(RaftGroupId groupId) {
  }

  /**
   * 描述：默认方法抛出 UnsupportedOperationException，表示当前实现不支持异步操作。
   * <p>
   * 用途：有些 Raft 服务器实现可能支持异步协议，而其他实现可能只支持同步协议。
   * 此方法的默认实现提示当前的实现不支持异步操作。如果某些实现支持异步操作，应该覆盖该方法。
   */
  default RaftServerAsynchronousProtocol async() {
    throw new UnsupportedOperationException(getClass().getName()
        + " does not support " + JavaUtils.getClassSimpleName(RaftServerAsynchronousProtocol.class));
  }
}
