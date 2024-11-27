package net.xdob.ratly.server;

import net.xdob.ratly.protocol.ClientInvocationId;
import net.xdob.ratly.statemachine.DataStream;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * 接口用于在 Raft 服务器中存储和管理 ClientInvocationId（客户端请求的唯一标识符）与 DataStream（数据流）的映射关系。
 * 它提供了类似于 java.util.Map 的方法，用于在特定的请求标识符上进行数据流的存取和删除操作。
 */
public interface DataStreamMap {
  /**
   * 类似于 {@link java.util.Map#computeIfAbsent(Object, Function)} 方法，
   * 目的是确保在给定的 invocationId 对应的 DataStream 不存在时，
   * 会异步创建一个新的 DataStream，而不是重复创建。常用于处理客户端发起的请求，并为每个请求提供独立的数据流。
   */
  CompletableFuture<DataStream> computeIfAbsent(ClientInvocationId invocationId,
      Function<ClientInvocationId, CompletableFuture<DataStream>> newDataStream);

  /**
   * 根据给定的 ClientInvocationId 移除映射中对应的 DataStream。返回一个 CompletableFuture<DataStream>。
   * 类似于 {@link java.util.Map#remove(Object)。
   */
  CompletableFuture<DataStream> remove(ClientInvocationId invocationId);
}
