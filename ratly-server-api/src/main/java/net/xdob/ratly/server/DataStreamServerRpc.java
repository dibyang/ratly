
package net.xdob.ratly.server;

import net.xdob.ratly.protocol.RaftPeer;

import java.io.Closeable;

/**
 * A server interface handling incoming streams
 * Relays those streams to other servers after persisting
 */
public interface DataStreamServerRpc extends ServerRpc, RaftPeer.Add, Closeable {
}
