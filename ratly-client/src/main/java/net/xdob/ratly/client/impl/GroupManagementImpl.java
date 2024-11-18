
package net.xdob.ratly.client.impl;

import net.xdob.ratly.client.api.GroupManagementApi;
import net.xdob.ratly.protocol.GroupInfoReply;
import net.xdob.ratly.protocol.GroupInfoRequest;
import net.xdob.ratly.protocol.GroupListReply;
import net.xdob.ratly.protocol.GroupListRequest;
import net.xdob.ratly.protocol.GroupManagementRequest;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftGroup;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.rpc.CallId;
import net.xdob.ratly.util.Preconditions;

import java.io.IOException;
import java.util.Objects;

class GroupManagementImpl implements GroupManagementApi {
  private final RaftPeerId server;
  private final RaftClientImpl client;

  GroupManagementImpl(RaftPeerId server, RaftClientImpl client) {
    this.server = Objects.requireNonNull(server, "server == null");
    this.client = Objects.requireNonNull(client, "client == null");
  }

  @Override
  public RaftClientReply add(RaftGroup newGroup, boolean format) throws IOException {
    Objects.requireNonNull(newGroup, "newGroup == null");

    final long callId = CallId.getAndIncrement();
    client.getClientRpc().addRaftPeers(newGroup.getPeers());
    return client.io().sendRequestWithRetry(
        () -> GroupManagementRequest.newAdd(client.getId(), server, callId, newGroup, format));
  }

  @Override
  public RaftClientReply remove(RaftGroupId groupId, boolean deleteDirectory, boolean renameDirectory)
      throws IOException {
    Objects.requireNonNull(groupId, "groupId == null");

    final long callId = CallId.getAndIncrement();
    return client.io().sendRequestWithRetry(
        () -> GroupManagementRequest.newRemove(client.getId(), server, callId, groupId,
            deleteDirectory, renameDirectory));
  }

  @Override
  public GroupListReply list() throws IOException {
    final long callId = CallId.getAndIncrement();
    final RaftClientReply reply = client.io().sendRequestWithRetry(
        () -> new GroupListRequest(client.getId(), server, client.getGroupId(), callId));
    Preconditions.assertTrue(reply instanceof GroupListReply, () -> "Unexpected reply: " + reply);
    return (GroupListReply)reply;
  }

  @Override
  public GroupInfoReply info(RaftGroupId groupId) throws IOException {
    final RaftGroupId gid = groupId != null? groupId: client.getGroupId();
    final long callId = CallId.getAndIncrement();
    final RaftClientReply reply = client.io().sendRequestWithRetry(
        () -> new GroupInfoRequest(client.getId(), server, gid, callId));
    Preconditions.assertTrue(reply instanceof GroupInfoReply, () -> "Unexpected reply: " + reply);
    return (GroupInfoReply)reply;
  }
}
