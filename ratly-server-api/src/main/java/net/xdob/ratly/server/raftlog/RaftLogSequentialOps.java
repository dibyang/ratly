package net.xdob.ratly.server.raftlog;

import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.protocol.exceptions.StateMachineException;
import net.xdob.ratly.server.RaftConfiguration;
import net.xdob.ratly.statemachine.TransactionContext;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.ReferenceCountedObject;
import net.xdob.ratly.util.StringUtils;
import net.xdob.ratly.util.function.CheckedSupplier;
import net.xdob.ratly.util.function.UncheckedAutoCloseableSupplier;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Sequential operations in {@link RaftLog}.
 *
 * All methods in this class MUST be invoked by a single thread at any time.
 * The threads can be different in different time.
 * The same thread may invoke any of the methods again and again.
 * In other words, two or more threads invoking these methods (the same method or different methods)
 * at the same time is not allowed since the sequence of invocations cannot be guaranteed.
 *
 * All methods in this class are asynchronous in the sense that the underlying I/O operations are asynchronous.
 *  Raft 日志中顺序操作的接口。
 *  1.线程安全的设计约束：
 *     所有方法必须由单一线程调用，不能并发执行。这是为了保证操作的顺序性。
 *     不同时间可以由不同线程调用这些方法，但同一时间只允许一个线程调用。
 *  2.异步操作支持：
 *     所有方法背后的 I/O 操作都是异步的。
 */
interface RaftLogSequentialOps {
  /**
   * 该类用于管理线程的顺序执行：
   *    利用 AtomicReference<Thread> 确保同一时刻只有一个线程可以执行操作。
   *    提供了 runSequentially 方法来执行顺序操作。如果另一个线程尝试调用这个方法，将抛出 IllegalStateException。
   */
  class Runner {
    private final Object name;
    private final AtomicReference<Thread> runner = new AtomicReference<>();

    Runner(Supplier<String> name) {
      this.name = StringUtils.stringSupplierAsObject(name);
    }

    /**
     * Run the given operation sequentially.
     * This method can be invoked by different threads but only one thread at any given time is allowed.
     * The same thread can call this method multiple times.
     * 1.检查当前线程是否已经是运行中的线程。
     * 2.如果当前线程是新的运行线程，执行操作并在完成后释放运行权。
     * 3.如果当前线程已经在运行，直接执行操作。
     * 4.如果是其他线程尝试运行，抛出异常。
     * @throws IllegalStateException if this runner is already running another operation.
     */
    <OUTPUT, THROWABLE extends Throwable> OUTPUT runSequentially(
        CheckedSupplier<OUTPUT, THROWABLE> operation) throws THROWABLE {
      final Thread current = Thread.currentThread();
      // update only if the runner is null
      final Thread previous = runner.getAndUpdate(prev -> prev != null? prev: current);
      if (previous == null) {
        // The current thread becomes the runner.
        try {
          return operation.get();
        } finally {
          // prev is expected to be current
          final Thread got = runner.getAndUpdate(prev -> prev != current? prev: null);
          Preconditions.assertTrue(got == current,
              () -> name + ": Unexpected runner " + got + " != " + current);
        }
      } else if (previous == current) {
        // The current thread is already the runner.
        return operation.get();
      } else {
        throw new IllegalStateException(
            name + ": Already running a method by " + previous + ", current=" + current);
      }
    }
  }

  /**
   * 追加一个新的日志条目（事务）。
   * Append asynchronously a log entry for the given term and transaction.
   * Used by the leader.
   *
   * Note that the underlying I/O operation is submitted but may not be completed when this method returns.
   *
   * @return the index of the new log entry.
   */
  long append(long term, TransactionContext transaction) throws StateMachineException;

  /**
   * 追加一个新的配置日志条目。
   * Append asynchronously a log entry for the given term and configuration
   * Used by the leader.
   *
   * Note that the underlying I/O operation is submitted but may not be completed when this method returns.
   *
   * @return the index of the new log entry.
   */
  long append(long term, RaftConfiguration configuration);

  /**
   * 追加元数据日志条目，通常是提交索引。
   * Append asynchronously a log entry for the given term and commit index
   * unless the given commit index is an index of a metadata entry
   * Used by the leader.
   *
   * Note that the underlying I/O operation is submitted but may not be completed when this method returns.
   *
   * @return the index of the new log entry if it is appended;
   *         otherwise, return {@link RaftLog#INVALID_LOG_INDEX}.
   */
  long appendMetadata(long term, long commitIndex);

  /**
   * 异步追加单个日志条目。
   * Append asynchronously an entry.
   * Used by the leader and the followers.
   */
  CompletableFuture<Long> appendEntry(LogEntryProto entry);

  /**
   * 废弃，推荐使用带 ReferenceCountedObject 的新版本。
   * @deprecated use {@link #appendEntry(ReferenceCountedObject, TransactionContext)}}.
   */
  @Deprecated
  default CompletableFuture<Long> appendEntry(LogEntryProto entry, TransactionContext context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Append asynchronously an entry.
   * Used for scenarios that there is a ReferenceCountedObject context for resource cleanup when the given entry
   * is no longer used/referenced by this log.
   */
  default CompletableFuture<Long> appendEntry(ReferenceCountedObject<LogEntryProto> entryRef,
      TransactionContext context) {
    return appendEntry(entryRef.get(), context);
  }

  /**
   * 废弃，推荐使用新的异步追加方法。
   * The same as append(Arrays.asList(entries)).
   *
   * @deprecated use {@link #append(ReferenceCountedObject)}.
   */
  @Deprecated
  default List<CompletableFuture<Long>> append(LogEntryProto... entries) {
    return append(Arrays.asList(entries));
  }

  /**
   * 废弃，推荐使用新的异步追加方法。
   * @deprecated use {@link #append(ReferenceCountedObject)}.
   */
  @Deprecated
  default List<CompletableFuture<Long>> append(List<LogEntryProto> entries) {
    throw new UnsupportedOperationException();
  }

  /**
   * 异步追加多个日志条目。
   * Append asynchronously all the given log entries.
   * Used by the followers.
   *
   * If an existing entry conflicts with a new one (same index but different terms),
   * delete the existing entry and all entries that follow it (§5.3).
   *
   * A reference counter is also submitted.
   * For each entry, implementations of this method should retain the counter, process it and then release.
   */
  default List<CompletableFuture<Long>> append(ReferenceCountedObject<List<LogEntryProto>> entriesRef) {
    try(UncheckedAutoCloseableSupplier<List<LogEntryProto>> entries = entriesRef.retainAndReleaseOnClose()) {
      return append(entries.get());
    }
  }

  /**
   * 异步截断日志到指定索引（包含）。
   * Truncate asynchronously the log entries till the given index (inclusively).
   * Used by the leader and the followers.
   */
  CompletableFuture<Long> truncate(long index);
}
