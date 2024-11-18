
package net.xdob.ratly.client;

import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.datastream.SupportedDataStreamType;
import net.xdob.ratly.protocol.RaftPeer;

/** A stream factory that does nothing when data stream is disabled. */
public class DisabledDataStreamClientFactory implements DataStreamClientFactory {
  public DisabledDataStreamClientFactory(Parameters parameters) {}

  @Override
  public SupportedDataStreamType getDataStreamType() {
    return SupportedDataStreamType.DISABLED;
  }

  @Override
  public DataStreamClientRpc newDataStreamClientRpc(RaftPeer server, RaftProperties properties) {
    return new DataStreamClientRpc() {
      @Override
      public void close() {}
    };
  }
}
