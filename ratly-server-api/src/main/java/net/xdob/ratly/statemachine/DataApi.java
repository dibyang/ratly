package net.xdob.ratly.statemachine;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.protocol.RaftClientRequest;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.ReferenceCountedObject;

import java.util.concurrent.CompletableFuture;

/**
 * DataApi 接口是一个可选的 API，用于管理 Raft 状态机外的数据。
 * 它为数据密集型的应用程序提供了高效的管理和访问数据的方式，支持零缓冲区复制和精简的 Raft 日志存储。
 * 这些方法提供了对日志条目数据的异步读写操作、数据流的创建和链接、数据的刷新和截断等操作，旨在增强 Raft 状态机的性能和灵活性。
 */
public interface DataApi {
  /**
   * A noop implementation of {@link DataApi}.
   */
  DataApi DEFAULT = new DataApi() {
  };

  /**
   * 功能：
   *    异步读取指定的状态机数据。传入的 LogEntryProto 代表日志条目，该方法返回一个 CompletableFuture<ByteString>，表示读取任务。
   * 默认实现会抛出 UnsupportedOperationException，表示该方法未实现。
   *
   * @return a future for the read task.
   */
  default CompletableFuture<ByteString> read(LogEntryProto entry) {
    throw new UnsupportedOperationException("This method is NOT supported.");
  }

  /**
   * 功能：
   *    与上面的 read(LogEntryProto entry) 方法类似，增加了一个 TransactionContext 参数，用于在读取时传递与事务相关的上下文信息。
   * 默认实现直接调用 read(entry)，并返回相同的结果。
   *
   * @return a future for the read task.
   */
  default CompletableFuture<ByteString> read(LogEntryProto entry, TransactionContext context) {
    return read(entry);
  }

  /**
   * 功能：
   *    异步读取状态机数据，并将读取的结果包装在  {@link ReferenceCountedObject} 中，
   *    客户端调用此方法后，必须调用 {@link ReferenceCountedObject#release()} 来释放资源。
   * 该方法在读取的结果包含需要释放的资源时非常有用。
   *
   * @return a future for the read task.
   * The result of the future is a {@link ReferenceCountedObject} wrapping the read result.
   * Client code of this method must call  {@link ReferenceCountedObject#release()} after use.
   */
  default CompletableFuture<ReferenceCountedObject<ByteString>> retainRead(LogEntryProto entry,
                                                                           TransactionContext context) {
    return read(entry, context).thenApply(r -> {
      if (r == null) {
        return null;
      }
      ReferenceCountedObject<ByteString> ref = ReferenceCountedObject.wrap(r);
      ref.retain();
      return ref;

    });
  }

  /**
   * 功能：
   * 异步写入日志条目到状态机。
   * 这是一个已弃用的方法，推荐使用带有 ReferenceCountedObject 参数的方法 {@link #write(ReferenceCountedObject, TransactionContext)}。
   * 默认实现直接返回一个已完成的 CompletableFuture。
   *
   * @return a future for the write task
   * @deprecated Applications should implement {@link #write(ReferenceCountedObject, TransactionContext)} instead.
   */
  @Deprecated
  default CompletableFuture<?> write(LogEntryProto entry) {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * 与上面的 write(LogEntryProto entry) 方法相似，增加了一个 TransactionContext 参数，用于处理与事务相关的数据。
   * 默认实现直接调用 write(entry)。
   *
   * @return a future for the write task
   * @deprecated Applications should implement {@link #write(ReferenceCountedObject, TransactionContext)} instead.
   */
  @Deprecated
  default CompletableFuture<?> write(LogEntryProto entry, TransactionContext context) {
    return write(entry);
  }

  /**
   * 功能：
   *    异步写入带有 ReferenceCountedObject 包装的日志条目。entryRef 是一个对日志条目的引用，
   *    调用者可以在此方法返回之前获取条目的副本，并在使用后释放引用。
   *    在实现中，如果需要使用条目进行异步操作或缓存，必须显式调用 {@link ReferenceCountedObject#retain()}
   *    和 {@link ReferenceCountedObject#release()} 来管理内存。
   *
   * @param entryRef Reference to a log entry.
   * @return a future for the write task
   */
  default CompletableFuture<?> write(ReferenceCountedObject<LogEntryProto> entryRef, TransactionContext context) {
    final LogEntryProto entry = entryRef.get();
    try {
      final LogEntryProto copy = LogEntryProto.parseFrom(entry.toByteString());
      return write(copy, context);
    } catch (InvalidProtocolBufferException e) {
      return JavaUtils.completeExceptionally(new IllegalStateException(
          "Failed to copy log entry " + TermIndex.valueOf(entry), e));
    }
  }

  /**
   * 功能：
   *    异步创建一个数据流 {@link DataStream} 用于流式传输状态机数据。可以使用请求的第一个消息作为创建流的标头。
   *    返回一个 CompletableFuture<DataStream>，表示异步任务，通常用于数据流的初始化。
   *
   * @return a future of the stream.
   */
  default CompletableFuture<DataStream> stream(RaftClientRequest request) {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * 功能：
   *    将给定的 DataStream 与指定的日志条目关联。
   *    如果流不可用（例如由于错误），则可以传入 null，并允许状态机自行恢复数据或将返回的 CompletableFuture 完成异常。
   *    该方法支持流的异步链接。
   *
   * @param stream the stream, which can possibly be null, to be linked.
   * @param entry  the log entry to be linked.
   * @return a future for the link task.
   */
  default CompletableFuture<?> link(DataStream stream, LogEntryProto entry) {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * 功能：
   *    异步将状态机数据刷新到指定的日志索引。该方法确保在 logIndex 索引之前的所有数据都已持久化到存储中。
   *    返回一个 CompletableFuture<Void>，表示刷新操作的异步任务。
   *
   * @param logIndex The log index to flush.
   * @return a future for the flush task.
   */
  default CompletableFuture<Void> flush(long logIndex) {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * 功能：
   *    异步截断状态机数据到指定的日志索引。如果对应的日志条目没有状态机数据，则此操作不做任何事。
   *    返回一个 CompletableFuture<Void>，表示截断操作的异步任务。
   *
   * @param logIndex The last log index after truncation.
   * @return a future for truncate task.
   */
  default CompletableFuture<Void> truncate(long logIndex) {
    return CompletableFuture.completedFuture(null);
  }
}
