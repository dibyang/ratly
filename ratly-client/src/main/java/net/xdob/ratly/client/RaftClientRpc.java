
package net.xdob.ratly.client;

import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftClientRequest;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.util.IOUtils;
import net.xdob.ratly.util.JavaUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** The client side rpc of a raft service. */
public interface RaftClientRpc extends RaftPeer.Add, Closeable {
  /** Async call to send a request. */
  default CompletableFuture<RaftClientReply> sendRequestAsync(RaftClientRequest request) {
    throw new UnsupportedOperationException(getClass() + " does not support this method.");
  }

  /** Async call to send a request. */
  default CompletableFuture<RaftClientReply> sendRequestAsyncUnordered(RaftClientRequest request) {
    throw new UnsupportedOperationException(getClass() + " does not support "
        + JavaUtils.getCurrentStackTraceElement().getMethodName());
  }

  /** Send a request. */
  RaftClientReply sendRequest(RaftClientRequest request) throws IOException;

  /**
   * Handle the given throwable. For example, try reconnecting.
   *
   * @return true if the given throwable is handled; otherwise, the call is an no-op, return false.
   */
  default boolean handleException(RaftPeerId serverId, Throwable t, boolean reconnect) {
    return false;
  }

  /**
   * Determine if the given throwable should be handled. For example, try reconnecting.
   *
   * @return true if the given throwable should be handled; otherwise, return false.
   */
  default boolean shouldReconnect(Throwable t) {
    return IOUtils.shouldReconnect(t);
  }
}
