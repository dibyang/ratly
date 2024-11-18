
package net.xdob.ratly.netty;

import net.xdob.ratly.client.ClientFactory;
import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.netty.client.NettyClientRpc;
import net.xdob.ratly.netty.server.NettyRpcService;
import net.xdob.ratly.protocol.ClientId;
import net.xdob.ratly.rpc.SupportedRpcType;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.ServerFactory;

public class NettyFactory implements ServerFactory, ClientFactory {
  public NettyFactory(Parameters parameters) {}

  @Override
  public SupportedRpcType getRpcType() {
    return SupportedRpcType.NETTY;
  }

  @Override
  public NettyRpcService newRaftServerRpc(RaftServer server) {
    return NettyRpcService.newBuilder().setServer(server).build();
  }

  @Override
  public NettyClientRpc newRaftClientRpc(ClientId clientId, RaftProperties properties) {
    return new NettyClientRpc(clientId, properties);
  }
}
