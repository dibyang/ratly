
package net.xdob.ratly.client;

import net.xdob.ratly.client.api.AsyncApi;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftClientRequest;

import java.util.concurrent.CompletableFuture;

/** An RPC interface which extends the user interface {@link AsyncApi}. */
public interface AsyncRpcApi extends AsyncApi {
  /**
   * Send the given forward-request asynchronously to the raft service.
   *
   * @param request The request to be forwarded.
   * @return a future of the reply.
   */
  CompletableFuture<RaftClientReply> sendForward(RaftClientRequest request);
}
