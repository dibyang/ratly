package net.xdob.ratly.statemachine;

import net.xdob.ratly.proto.raft.LogEntryProto;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * 接口用于管理流式状态机数据的处理和清理。
 * 它定义了几个关键方法，帮助开发者管理流数据的生命周期，特别是在流式处理过程中可能出现的错误或需要异步清理的场景。
 */
public interface DataStream {
  /**
   * 功能：
   * 返回一个 DataChannel，用于流式传输状态机数据。
   *    该方法通常用于获取一个用于数据写入或读取的通道，帮助流数据在不同的组件之间传输。
   * 返回值：
   *    DataChannel：用于数据流的通道，它可能是一个缓冲区、文件或其他数据存储。
   * @return a channel for streaming state machine data.
   */
  DataChannel getDataChannel();

  /**
   * Clean up asynchronously this stream.
   * <p>
   * 功能：
   *    在发生错误时调用该方法清理关联的资源。
   *    如果流还没有被链接到状态机（即没有通过 {@link DataApi#link(DataStream, LogEntryProto)} 链接），
   *    状态机可以选择删除数据或将数据保留以便以后恢复。
   *    如果流已经被链接到状态机，数据必须保持不变，不能被删除。
   * 返回值：
   *    CompletableFuture<?>：该方法返回一个异步任务，表示清理操作的结果。这使得清理任务可以是异步执行的，从而不会阻塞主线程。
   *
   * @return a future for the cleanup task.
   */
  CompletableFuture<?> cleanUp();

  /**
   * 功能：
   *    返回一个 {@link Executor}，用于执行与流相关的任务（如写入操作、数据清理等）。
   *    如果该方法返回 null，则使用默认的 {@link Executor}。
   * 返回值：
   *    Executor：执行流任务的线程池或执行器。
   *    默认情况下，返回 null 表示使用默认的执行器。
   * @return an {@link Executor} for executing the streaming tasks of this stream.
   */
  default Executor getExecutor() {
    return null;
  }
}
