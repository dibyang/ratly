package net.xdob.ratly.client.impl;

import net.xdob.ratly.client.api.DRpcApi;
import net.xdob.ratly.protocol.*;
import net.xdob.ratly.rpc.CallId;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.function.SerialFunction;

import java.io.IOException;
import java.util.Objects;

public class DRpcApiImpl implements DRpcApi {
  private final RaftPeerId server;
  private final RaftClientImpl client;

  DRpcApiImpl(RaftPeerId server, RaftClientImpl client) {
    this.server = Objects.requireNonNull(server, "server == null");
    this.client = Objects.requireNonNull(client, "client == null");
  }


  @Override
  public <T, R> R invokeRpc(SerialFunction<T, R> fun) {
    return invokeRpc(BeanTarget.valueOf(null), fun);
  }

  @Override
  public <T, R> R invokeRpc(Class<T> clazz, SerialFunction<T, R> fun) {
    return invokeRpc(BeanTarget.valueOf(clazz), fun);
  }


  @Override
  public <T, R> R invokeRpc(BeanTarget<T> target, SerialFunction<T, R> fun)  {
    final RaftGroupId gid =  client.getGroupId();
    final long callId = CallId.getAndIncrement();
    final RaftClientReply reply;
    try {
      reply = client.io().sendRequestWithRetry(
          () -> new DRpcRequest<>(client.getId(), server, gid, callId, target, fun));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Preconditions.assertTrue(reply instanceof DRpcReply, () -> "Unexpected reply: " + reply);
    DRpcReply<R> r = (DRpcReply<R>)reply;
    return r.getData();
  }
}
