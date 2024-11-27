package net.xdob.ratly.server.protocol;

import net.xdob.ratly.proto.raft.ReadIndexRequestProto;
import net.xdob.ratly.proto.raft.ReadIndexReplyProto;
import net.xdob.ratly.proto.raft.AppendEntriesReplyProto;
import net.xdob.ratly.proto.raft.AppendEntriesRequestProto;
import net.xdob.ratly.util.ReferenceCountedObject;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Raft 协议中的一个扩展接口，专注于异步处理操作。这种设计可以提高系统的性能和响应性，特别是在高并发环境中。
 * 核心功能
 * 提供了异步实现的日志追加和读索引操作。
 * 支持使用引用计数（ReferenceCountedObject）的请求对象，以更高效地管理资源。
 */
public interface RaftServerAsynchronousProtocol {

  /**
   * It is recommended to override {@link #appendEntriesAsync(ReferenceCountedObject)} instead.
   * Then, it does not have to override this method.
   * 功能：
   * 异步处理日志追加请求。
   * 默认实现抛出 UnsupportedOperationException，建议开发者覆盖另一种使用引用计数的异步方法。
   * 仅提供默认实现，主要为了向后兼容。如果用户实现了 appendEntriesAsync(ReferenceCountedObject)，可以省略对这个方法的覆盖。
   * @param request 包含日志追加请求的元数据。
   * @return 日志追加请求的异步响应。
   */
  default CompletableFuture<AppendEntriesReplyProto> appendEntriesAsync(AppendEntriesRequestProto request)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * A referenced counted request is submitted from a client for processing.
   * Implementations of this method should retain the request, process it and then release it.
   * The request may be retained even after the future returned by this method has completed.
   * 处理带引用计数的日志追加请求。
   * 默认实现会尝试保留引用，处理请求后再释放引用。
   * @param requestRef 一个带有引用计数的 AppendEntriesRequestProto 对象。
   * @return 日志追加请求的异步响应。
   * @see ReferenceCountedObject
   */
  default CompletableFuture<AppendEntriesReplyProto> appendEntriesAsync(
      ReferenceCountedObject<AppendEntriesRequestProto> requestRef) throws IOException {
    // Default implementation for backward compatibility.
    try {
      return appendEntriesAsync(requestRef.retain());
    } finally {
      requestRef.release();
    }
  }

  /**
   * 异步处理读索引操作。
   * 提供一种轻量级方法，用于从集群中读取数据时的一致性保障。
   * @param request 包含请求的元数据。
   * @return 读索引请求的异步响应。
   */
  CompletableFuture<ReadIndexReplyProto> readIndexAsync(ReadIndexRequestProto request)
      throws IOException;
}
