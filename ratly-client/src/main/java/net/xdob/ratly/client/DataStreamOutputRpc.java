
package net.xdob.ratly.client;

import net.xdob.ratly.client.api.DataStreamOutput;
import net.xdob.ratly.protocol.DataStreamReply;

import java.util.concurrent.CompletableFuture;

/** An RPC interface which extends the user interface {@link DataStreamOutput}. */
public interface DataStreamOutputRpc extends DataStreamOutput {
  /** Get the future of the header request. */
  CompletableFuture<DataStreamReply> getHeaderFuture();
}
