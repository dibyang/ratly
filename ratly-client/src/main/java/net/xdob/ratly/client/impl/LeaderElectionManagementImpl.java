

package net.xdob.ratly.client.impl;

import net.xdob.ratly.client.api.LeaderElectionManagementApi;
import net.xdob.ratly.protocol.LeaderElectionManagementRequest;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.rpc.CallId;

import java.io.IOException;
import java.util.Objects;

public class LeaderElectionManagementImpl implements LeaderElectionManagementApi {

  private final RaftClientImpl client;
  private final RaftPeerId server;

  LeaderElectionManagementImpl(RaftPeerId server, RaftClientImpl client) {
    this.server =  Objects.requireNonNull(server, "server == null");
    this.client = Objects.requireNonNull(client, "client == null");
  }
  @Override
  public RaftClientReply pause() throws IOException {
    final long callId = CallId.getAndIncrement();
    return client.io().sendRequestWithRetry(() -> LeaderElectionManagementRequest.newPause(client.getId(),
        server, client.getGroupId(), callId));
  }

  @Override
  public RaftClientReply resume() throws IOException {
    final long callId = CallId.getAndIncrement();
    return client.io().sendRequestWithRetry(() -> LeaderElectionManagementRequest.newResume(client.getId(),
        server, client.getGroupId(), callId));
  }
}
