
package net.xdob.ratly.netty;

import net.xdob.ratly.client.DataStreamClientFactory;
import net.xdob.ratly.client.DataStreamClientRpc;
import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.datastream.SupportedDataStreamType;
import net.xdob.ratly.netty.client.NettyClientStreamRpc;
import net.xdob.ratly.netty.server.NettyServerStreamRpc;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.server.DataStreamServerFactory;
import net.xdob.ratly.server.DataStreamServerRpc;
import net.xdob.ratly.server.RaftServer;

import java.util.Optional;

public class NettyDataStreamFactory implements DataStreamServerFactory, DataStreamClientFactory {
  private final Parameters parameters;

  public NettyDataStreamFactory(Parameters parameters) {
    this.parameters = Optional.ofNullable(parameters).orElseGet(Parameters::new);
  }

  @Override
  public SupportedDataStreamType getDataStreamType() {
    return SupportedDataStreamType.NETTY;
  }

  @Override
  public DataStreamClientRpc newDataStreamClientRpc(RaftPeer server, RaftProperties properties) {
    return new NettyClientStreamRpc(server, NettyConfigKeys.DataStream.Client.tlsConf(parameters), properties);
  }

  @Override
  public DataStreamServerRpc newDataStreamServerRpc(RaftServer server) {
    return new NettyServerStreamRpc(server, parameters);
  }
}
