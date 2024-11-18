
package net.xdob.ratly.client.impl;

import net.xdob.ratly.client.api.AdminApi;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.protocol.SetConfigurationRequest;
import net.xdob.ratly.protocol.TransferLeadershipRequest;
import net.xdob.ratly.rpc.CallId;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

class AdminImpl implements AdminApi {
  private final RaftClientImpl client;

  AdminImpl(RaftClientImpl client) {
    this.client = Objects.requireNonNull(client, "client == null");
  }

  public RaftClientReply setConfiguration(SetConfigurationRequest.Arguments arguments) throws IOException {
    List<RaftPeer> peersInNewConf = arguments.getServersInNewConf();
    Objects.requireNonNull(peersInNewConf, "peersInNewConf == null");

    final long callId = CallId.getAndIncrement();
    // also refresh the rpc proxies for these peers
    client.getClientRpc().addRaftPeers(peersInNewConf);
    return client.io().sendRequestWithRetry(() -> new SetConfigurationRequest(
        client.getId(), client.getLeaderId(), client.getGroupId(), callId, arguments));
  }

  @Override
  public RaftClientReply transferLeadership(
      RaftPeerId newLeader, RaftPeerId leaderId, long timeoutMs) throws IOException {
    final long callId = CallId.getAndIncrement();
    return client.io().sendRequestWithRetry(() -> new TransferLeadershipRequest(
        client.getId(), leaderId == null ? client.getLeaderId() : leaderId,
        client.getGroupId(), callId, newLeader, timeoutMs));
  }
}
