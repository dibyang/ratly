
package net.xdob.ratly.server;

import net.xdob.ratly.protocol.ClientInvocationId;
import net.xdob.ratly.statemachine.StateMachine.DataStream;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** A {@link ClientInvocationId}-to-{@link DataStream} map. */
public interface DataStreamMap {
  /** Similar to {@link java.util.Map#computeIfAbsent(Object, Function)}. */
  CompletableFuture<DataStream> computeIfAbsent(ClientInvocationId invocationId,
      Function<ClientInvocationId, CompletableFuture<DataStream>> newDataStream);

  /** Similar to {@link java.util.Map#remove(Object). */
  CompletableFuture<DataStream> remove(ClientInvocationId invocationId);
}
