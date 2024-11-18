
package net.xdob.ratly.client;

import net.xdob.ratly.RaftConfigKeys;
import net.xdob.ratly.client.impl.ClientImplUtils;
import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.datastream.SupportedDataStreamType;
import net.xdob.ratly.protocol.ClientId;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftPeer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Objects;
import java.util.Optional;

/**
 * A user interface extending {@link DataStreamRpcApi}.
 */
public interface DataStreamClient extends DataStreamRpcApi, Closeable {
  Logger LOG = LoggerFactory.getLogger(DataStreamClient.class);

  /** Return the rpc client instance **/
  DataStreamClientRpc getClientRpc();

  static Builder newBuilder() {
    return newBuilder(null);
  }

  static Builder newBuilder(RaftClient client) {
    return new Builder(client);
  }

  /** To build {@link DataStreamClient} objects */
  final class Builder {
    private RaftPeer dataStreamServer;
    private DataStreamClientRpc dataStreamClientRpc;
    private RaftProperties properties;
    private Parameters parameters;
    private RaftGroupId groupId;
    private ClientId clientId;

    private final RaftClient client;

    private Builder(RaftClient client) {
      this.client = client;
    }

    public DataStreamClient build() {
      Objects.requireNonNull(dataStreamServer, "The 'dataStreamServer' field is not initialized.");
      if (properties != null) {
        if (dataStreamClientRpc == null) {
          final SupportedDataStreamType type = RaftConfigKeys.DataStream.type(properties, LOG::info);
          dataStreamClientRpc = DataStreamClientFactory.newInstance(type, parameters)
              .newDataStreamClientRpc(dataStreamServer, properties);
        }
      }
      if (client != null) {
        return ClientImplUtils.newDataStreamClient(
            client, dataStreamServer, dataStreamClientRpc, properties);
      }
      return ClientImplUtils.newDataStreamClient(
          Optional.ofNullable(clientId).orElseGet(ClientId::randomId),
          groupId, dataStreamServer, dataStreamClientRpc, properties);
    }

    public Builder setClientId(ClientId clientId) {
      this.clientId = clientId;
      return this;
    }

    public Builder setGroupId(RaftGroupId groupId) {
      this.groupId = groupId;
      return this;
    }

    public Builder setDataStreamServer(RaftPeer dataStreamServer) {
      this.dataStreamServer = dataStreamServer;
      return this;
    }

    public Builder setDataStreamClientRpc(DataStreamClientRpc dataStreamClientRpc) {
      this.dataStreamClientRpc = dataStreamClientRpc;
      return this;
    }

    public Builder setParameters(Parameters parameters) {
      this.parameters = parameters;
      return this;
    }

    public Builder setProperties(RaftProperties properties) {
      this.properties = properties;
      return this;
    }
  }
}
