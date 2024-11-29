package net.xdob.ratly.server.impl;

import net.xdob.ratly.protocol.ClientInvocationId;
import net.xdob.ratly.server.DataStreamMap;
import net.xdob.ratly.statemachine.DataStream;
import net.xdob.ratly.util.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

class DataStreamMapImpl implements DataStreamMap {
  public static final Logger LOG = LoggerFactory.getLogger(DataStreamMapImpl.class);

  private final String name;
  private final ConcurrentMap<ClientInvocationId, CompletableFuture<DataStream>> map = new ConcurrentHashMap<>();

  DataStreamMapImpl(Object name) {
    this.name = name + "-" + JavaUtils.getClassSimpleName(getClass());
  }

  @Override
  public CompletableFuture<DataStream> remove(ClientInvocationId invocationId) {
    return map.remove(invocationId);
  }

  @Override
  public CompletableFuture<DataStream> computeIfAbsent(ClientInvocationId invocationId,
      Function<ClientInvocationId, CompletableFuture<DataStream>> newDataStream) {
    return map.computeIfAbsent(invocationId, newDataStream);
  }

  @Override
  public String toString() {
    return name;
  }
}
