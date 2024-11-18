
package net.xdob.ratly.client.impl;

import net.xdob.ratly.client.DataStreamClient;
import net.xdob.ratly.client.DataStreamClientRpc;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.client.RaftClientRpc;
import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.protocol.RaftGroup;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.retry.RetryPolicy;
import net.xdob.ratly.protocol.ClientId;
import net.xdob.ratly.protocol.RaftPeerId;

/** Client utilities for internal use. */
public interface ClientImplUtils {
  @SuppressWarnings("checkstyle:ParameterNumber")
  static RaftClient newRaftClient(ClientId clientId, RaftGroup group,
      RaftPeerId leaderId, RaftPeer primaryDataStreamServer, RaftClientRpc clientRpc, RetryPolicy retryPolicy,
      RaftProperties properties, Parameters parameters) {
    return new RaftClientImpl(clientId, group, leaderId, primaryDataStreamServer, clientRpc, retryPolicy,
        properties, parameters);
  }

  static DataStreamClient newDataStreamClient(ClientId clientId, RaftGroupId groupId, RaftPeer primaryDataStreamServer,
      DataStreamClientRpc dataStreamClientRpc, RaftProperties properties) {
    return new DataStreamClientImpl(clientId, groupId, primaryDataStreamServer, dataStreamClientRpc, properties);
  }

  static DataStreamClient newDataStreamClient(RaftClient client, RaftPeer primaryDataStreamServer,
      DataStreamClientRpc dataStreamClientRpc, RaftProperties properties) {
    return new DataStreamClientImpl(client, primaryDataStreamServer, dataStreamClientRpc, properties);
  }
}
