
package net.xdob.ratly.server;

import net.xdob.ratly.protocol.RaftPeer;

import java.io.Closeable;

/**
 * 处理传入流的服务器接口，
 * 在持久化后将这些流中继到其他服务器
 */
public interface DataStreamServerRpc extends ServerRpc, RaftPeer.Add, Closeable {
}
