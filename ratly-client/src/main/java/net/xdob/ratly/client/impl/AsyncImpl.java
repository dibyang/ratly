
package net.xdob.ratly.client.impl;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import net.xdob.ratly.client.AsyncRpcApi;
import net.xdob.ratly.proto.RaftProtos;
import net.xdob.ratly.proto.RaftProtos.ReplicationLevel;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftClientRequest;
import net.xdob.ratly.protocol.RaftPeerId;

/** Async api implementations. */
class AsyncImpl implements AsyncRpcApi {
  private final RaftClientImpl client;

  AsyncImpl(RaftClientImpl client) {
    this.client = Objects.requireNonNull(client, "client == null");
  }

  CompletableFuture<RaftClientReply> send(
      RaftClientRequest.Type type, Message message, RaftPeerId server) {
    return client.getOrderedAsync().send(type, message, server);
  }

  @Override
  public CompletableFuture<RaftClientReply> send(Message message, ReplicationLevel replication) {
    return send(RaftClientRequest.writeRequestType(replication), message, null);
  }

  @Override
  public CompletableFuture<RaftClientReply> sendReadOnly(Message message, RaftPeerId server) {
    return send(RaftClientRequest.readRequestType(), message, server);
  }

  @Override
  public CompletableFuture<RaftClientReply> sendReadAfterWrite(Message message) {
    return send(RaftClientRequest.readAfterWriteConsistentRequestType(), message, null);
  }

  @Override
  public CompletableFuture<RaftClientReply> sendReadOnlyNonLinearizable(Message message) {
    return send(RaftClientRequest.readRequestType(true), message, null);
  }

  @Override
  public CompletableFuture<RaftClientReply> sendReadOnlyUnordered(Message message, RaftPeerId server) {
    return UnorderedAsync.send(RaftClientRequest.readRequestType(), message, server, client);
  }

  @Override
  public CompletableFuture<RaftClientReply> sendStaleRead(Message message, long minIndex, RaftPeerId server) {
    return send(RaftClientRequest.staleReadRequestType(minIndex), message, server);
  }

  @Override
  public CompletableFuture<RaftClientReply> watch(long index, ReplicationLevel replication) {
    return UnorderedAsync.send(RaftClientRequest.watchRequestType(index, replication), null, null, client);
  }

  @Override
  public CompletableFuture<RaftClientReply> sendForward(RaftClientRequest request) {
    final RaftProtos.RaftClientRequestProto proto = ClientProtoUtils.toRaftClientRequestProto(request);
    return send(RaftClientRequest.forwardRequestType(), Message.valueOf(proto.toByteString()), null);
  }
}
