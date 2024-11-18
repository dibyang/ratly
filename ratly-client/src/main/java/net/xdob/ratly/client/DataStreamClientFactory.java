

package net.xdob.ratly.client;

import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.datastream.DataStreamFactory;
import net.xdob.ratly.datastream.DataStreamType;
import net.xdob.ratly.protocol.RaftPeer;

/**
 * A factory to create streaming client.
 */
public interface DataStreamClientFactory extends DataStreamFactory {
  static DataStreamClientFactory newInstance(DataStreamType type, Parameters parameters) {
    final DataStreamFactory dataStreamFactory = type.newClientFactory(parameters);
    if (dataStreamFactory instanceof DataStreamClientFactory) {
      return (DataStreamClientFactory) dataStreamFactory;
    }
    throw new ClassCastException("Cannot cast " + dataStreamFactory.getClass()
        + " to " + DataStreamClientFactory.class + "; stream type is " + type);
  }

  DataStreamClientRpc newDataStreamClientRpc(RaftPeer server, RaftProperties properties);
}
