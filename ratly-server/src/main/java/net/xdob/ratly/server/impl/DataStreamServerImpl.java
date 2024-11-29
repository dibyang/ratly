package net.xdob.ratly.server.impl;

import java.io.IOException;
import net.xdob.ratly.RaftConfigKeys;
import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.datastream.SupportedDataStreamType;
import net.xdob.ratly.server.DataStreamServer;
import net.xdob.ratly.server.DataStreamServerFactory;
import net.xdob.ratly.server.DataStreamServerRpc;
import net.xdob.ratly.server.RaftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DataStreamServerImpl implements DataStreamServer {
  public static final Logger LOG = LoggerFactory.getLogger(DataStreamServerImpl.class);

  private final DataStreamServerRpc serverRpc;

  DataStreamServerImpl(RaftServer server, Parameters parameters) {
    final SupportedDataStreamType type = RaftConfigKeys.DataStream.type(server.getProperties(), LOG::info);
    this.serverRpc = DataStreamServerFactory.newInstance(type, parameters).newDataStreamServerRpc(server);
  }

  @Override
  public DataStreamServerRpc getServerRpc() {
    return serverRpc;
  }

  @Override
  public void close() throws IOException {
    serverRpc.close();
  }
}
