
package net.xdob.ratly.server;

import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.datastream.DataStreamFactory;
import net.xdob.ratly.datastream.DataStreamType;

/** A {@link DataStreamFactory} to create server-side objects. */
public interface DataStreamServerFactory extends DataStreamFactory {
  static DataStreamServerFactory newInstance(DataStreamType type, Parameters parameters) {
    final DataStreamFactory dataStreamFactory = type.newServerFactory(parameters);
    if (dataStreamFactory instanceof DataStreamServerFactory) {
      return (DataStreamServerFactory)dataStreamFactory;
    }
    throw new ClassCastException("Cannot cast " + dataStreamFactory.getClass()
        + " to " + DataStreamServerFactory.class + "; rpc type is " + type);
  }

  /** Create a new {@link DataStreamServerRpc}. */
  DataStreamServerRpc newDataStreamServerRpc(RaftServer server);
}
