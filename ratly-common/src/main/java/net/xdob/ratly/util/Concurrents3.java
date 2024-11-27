package net.xdob.ratly.util;

import net.xdob.ratly.util.function.CheckedConsumer;
import net.xdob.ratly.util.function.CheckedFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Utilities related to concurrent programming.
 */
public interface Concurrents3 {
  /**
   * 这个方法类似于 {@link AtomicReference#updateAndGet(java.util.function.UnaryOperator)}，
   * 但是它支持检查并处理可能抛出的检查型异常（THROWABLE）。
   * 它接受一个 AtomicReference<E> 和一个 CheckedFunction<E, E, THROWABLE> 类型的更新函数。
   * CheckedFunction 允许你提供一个可能抛出检查型异常的更新操作。
   * 如果更新函数抛出了异常，该异常将被捕获并重新抛出，确保操作的原子性，同时处理检查型异常。
   * 使用场景：适用于需要在更新引用时同时处理检查型异常的情况。
   */
  static <E, THROWABLE extends Throwable> E updateAndGet(AtomicReference<E> reference,
      CheckedFunction<E, E, THROWABLE> update) throws THROWABLE {
    final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
    final E updated = reference.updateAndGet(value -> {
      try {
        return update.apply(value);
      } catch (Error | RuntimeException e) {
        throw e;
      } catch (Throwable t) {
        throwableRef.set(t);
        return value;
      }
    });
    @SuppressWarnings("unchecked")
    final THROWABLE t = (THROWABLE) throwableRef.get();
    if (t != null) {
      throw t;
    }
    return updated;
  }

  /**
   * 该方法创建一个新的 {@link ThreadFactory}，并且所有由该工厂创建的线程都会有指定的前缀（namePrefix）作为线程名。
   * 它使用 AtomicInteger 来确保每个线程都有一个唯一的 ID。
   * 使用场景：当你需要为线程指定一致的命名规则时，特别是在调试或日志记录时，使用命名线程会更方便。
   * @param namePrefix the prefix used in the name of the threads created.
   * @return a new {@link ThreadFactory}.
   */
  static ThreadFactory newThreadFactory(String namePrefix) {
    final AtomicInteger numThread = new AtomicInteger();
    return runnable -> {
      final int id = numThread.incrementAndGet();
      final Thread t = new Thread(runnable);
      t.setName(namePrefix + "-thread" + id);
      return t;
    };
  }

  /**
    * 该方法创建一个新的 {@link ExecutorService}，并且该服务只会启动一个线程执行任务，且线程的名称是你指定的 name。
   * 使用场景：当你需要一个单线程的执行器，并且希望这个线程有特定的名称，便于调试时使用。
    *
    * @param name the thread name for only one thread.
    * @return a new {@link ExecutorService}.
    */
  static ExecutorService newSingleThreadExecutor(String name) {
      return Executors.newSingleThreadExecutor(runnable -> {
          final Thread t = new Thread(runnable);
          t.setName(name);
          return t;
        });
  }

  /**
   * 类似于  {@link Executors#newCachedThreadPool(ThreadFactory)}，
   * 但是它额外接受一个 maximumPoolSize 参数。如果 maximumPoolSize 为 0，它的行为与 newCachedThreadPool() 完全相同。
   * 它会创建一个核心池大小为 0，最大池大小为 maximumPoolSize 的 ThreadPoolExecutor。也就是说，它会根据需要创建线程，但线程数有上限。
   * 使用场景：当你需要一个带有线程数上限的缓存线程池时，可以用这个方法控制最大线程数。
   *
   * @param maximumPoolSize the maximum number of threads to allow in the pool.
   *                        When maximumPoolSize == 0, this method is the same as
   *                        {@link Executors#newCachedThreadPool(ThreadFactory)}.
   * @return a new {@link ExecutorService}.
   */
  static ExecutorService newCachedThreadPool(int maximumPoolSize, ThreadFactory threadFactory) {
    return maximumPoolSize == 0? Executors.newCachedThreadPool(threadFactory)
        : new ThreadPoolExecutor(0, maximumPoolSize, 60L, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(), threadFactory);
  }

  /**
   * 这个方法可以根据传入的 cached 参数来选择创建一个缓存线程池或固定线程池。它还允许你指定最大线程数和线程名称前缀。
   * 使用场景：当你需要在固定线程池和缓存线程池之间选择时，可以使用这个方法，根据实际需求来灵活配置线程池。
   *
   * @param cached Use cached thread pool?  If not, use a fixed thread pool.
   * @param maximumPoolSize the maximum number of threads to allow in the pool.
   * @param namePrefix the prefix used in the name of the threads created.
   * @return a new {@link ExecutorService}.
   */
  static ExecutorService newThreadPoolWithMax(boolean cached, int maximumPoolSize, String namePrefix) {
    final ThreadFactory f = newThreadFactory(namePrefix);
    return cached ? newCachedThreadPool(maximumPoolSize, f)
        : Executors.newFixedThreadPool(maximumPoolSize, f);
  }

  /**
   * Shutdown the given executor and wait for its termination.
   *
   * @param executor The executor to be shut down.
   */
  static void shutdownAndWait(ExecutorService executor) {
    shutdownAndWait(TimeDuration.ONE_DAY, executor, timeout -> {
      throw new IllegalStateException(executor.getClass().getName() + " shutdown timeout in " + timeout);
    });
  }

  static void shutdownAndWait(TimeDuration waitTime, ExecutorService executor, Consumer<TimeDuration> timoutHandler) {
    executor.shutdown();
    try {
      if (executor.awaitTermination(waitTime.getDuration(), waitTime.getUnit())) {
        return;
      }
    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
      return;
    }
    if (timoutHandler != null) {
      timoutHandler.accept(waitTime);
    }
  }

  /**
   * 这个方法是对 Collection.parallelStream() 的扩展，支持异步执行，并且可以指定执行操作的线程池。它还允许处理带有检查异常的操作。
   * 它有多个重载版本，可以接受流、集合或整数范围作为输入，并在异步执行时处理每个元素。
   * 使用场景：当你需要并行处理集合或流中的每个元素时，并且每个操作可能会抛出异常时，这个方法非常有用。
   * <p>
   * The same as {@link Collection#parallelStream()}.forEach(action) except that
   * (1) this method is asynchronous,
   * (2) this method has an executor parameter, and
   * (3) the action can throw a checked exception.
   *
   * @param stream The stream to be processed.
   * @param size The estimated size of the stream.
   * @param action To act on each element in the stream.
   * @param executor To execute the action.
   * @param <E> The element type.
   * @param <THROWABLE> the exception type.
   *
   * @return a {@link CompletableFuture} that is completed
   *         when the action is completed for each element in the collection.
   *         When the action throws an exception, the future will be completed exceptionally.
   *
   * @see Collection#parallelStream()
   * @see Stream#forEach(Consumer)
   */
  static <E, THROWABLE extends Throwable> CompletableFuture<Void> parallelForEachAsync(
      Stream<E> stream, int size, CheckedConsumer<? super E, THROWABLE> action, Executor executor) {
    final List<CompletableFuture<E>> futures = new ArrayList<>(size);
    stream.forEach(element -> {
      final CompletableFuture<E> f = new CompletableFuture<>();
      futures.add(f);
      executor.execute(() -> accept(action, element, f));
    });
    return JavaUtils.allOf(futures);
  }

  /** The same as parallelForEachAsync(collection.stream(), collection.size(), action, executor). */
  static <E, THROWABLE extends Throwable> CompletableFuture<Void> parallelForEachAsync(
      Collection<E> collection, CheckedConsumer<? super E, THROWABLE> action, Executor executor) {
    return parallelForEachAsync(collection.stream(), collection.size(), action, executor);
  }

  /** The same as parallelForEachAsync(collection.stream(), collection.size(), action, executor). */
  static <THROWABLE extends Throwable> CompletableFuture<Void> parallelForEachAsync(
      int size, CheckedConsumer<Integer, THROWABLE> action, Executor executor) {
    final AtomicInteger i = new AtomicInteger();
    return parallelForEachAsync(Stream.generate(i::getAndIncrement).limit(size), size, action, executor);
  }

  static <E, THROWABLE extends Throwable> void accept(
      CheckedConsumer<? super E, THROWABLE> action, E element, CompletableFuture<E> f) {
    try {
      action.accept(element);
      f.complete(element);
    } catch (Throwable t) {
      f.completeExceptionally(t);
    }
  }
}
