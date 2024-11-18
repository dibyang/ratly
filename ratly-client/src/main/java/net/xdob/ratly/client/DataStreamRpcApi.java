

package net.xdob.ratly.client;

import net.xdob.ratly.client.api.DataStreamApi;
import net.xdob.ratly.client.api.DataStreamOutput;
import net.xdob.ratly.protocol.RaftClientRequest;

/** An RPC interface which extends the user interface {@link DataStreamApi}. */
public interface DataStreamRpcApi extends DataStreamApi {
  /** Create a stream for primary server to send data to peer server. */
  DataStreamOutput stream(RaftClientRequest request);
}
