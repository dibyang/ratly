package net.xdob.ratly.client;

import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.protocol.ClientId;
import net.xdob.ratly.rpc.RpcFactory;

/** A factory interface for creating client components. */
public interface ClientFactory extends RpcFactory {
  static ClientFactory cast(RpcFactory rpcFactory) {
    if (rpcFactory instanceof ClientFactory) {
      return (ClientFactory)rpcFactory;
    }
    throw new ClassCastException("Cannot cast " + rpcFactory.getClass()
        + " to " + ClientFactory.class
        + "; rpc type is " + rpcFactory.getRpcType());
  }

  /** Create a {@link RaftClientRpc}. */
  RaftClientRpc newRaftClientRpc(ClientId clientId, RaftProperties properties);
}
