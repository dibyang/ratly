package net.xdob.ratly.client.impl;

import net.xdob.ratly.client.api.SnapshotManagementApi;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.protocol.SnapshotManagementRequest;
import net.xdob.ratly.rpc.CallId;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

class SnapshotManagementImpl implements SnapshotManagementApi {
  private final RaftClientImpl client;
  private final RaftPeerId server;

  SnapshotManagementImpl(RaftPeerId server, RaftClientImpl client) {
    this.server = server;
    this.client = Objects.requireNonNull(client, "client == null");
  }

  @Override
  public RaftClientReply create(long creationGap, long timeoutMs) throws IOException {
    final long callId = CallId.getAndIncrement();
    return client.io().sendRequestWithRetry(() -> SnapshotManagementRequest.newCreate(client.getId(),
        Optional.ofNullable(server).orElseGet(client::getLeaderId),
        client.getGroupId(), callId, timeoutMs, creationGap));
  }
}
