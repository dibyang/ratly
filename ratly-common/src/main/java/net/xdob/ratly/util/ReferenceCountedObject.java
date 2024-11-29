package net.xdob.ratly.util;

import net.xdob.ratly.util.function.UncheckedAutoCloseableSupplier;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 接口提供了一种基于引用计数管理对象生命周期的机制，可以确保对象的资源在使用后正确释放，并防止资源泄漏。
 * <p>
 * 必须释放：所有调用 retain() 的地方都必须匹配调用 release()，以避免资源泄漏。
 * <p>
 * 完全释放后不可再用：对象引用计数归零后，不可再次访问或保留。
 * <p>
 * 复合引用：通过 delegateFrom 方法，可以管理多个对象之间复杂的引用依赖关系。
 *
 * @param <T> The object type.
 */
public interface ReferenceCountedObject<T> {

  /**
   * 获取被包装的对象。
   * @return the object.
   */
  T get();

  /**
   * 增加对象的引用计数，表示需要继续保留对象的使用权。
   * The reference count will be increased by 1.
   * <p>
   * The {@link #release()} method must be invoked afterward.
   * Otherwise, the object is not returned, and it will cause a resource leak.
   *
   * @return the object.
   */
  T retain();

  /**
   * 返回一个实现了 UncheckedAutoCloseableSupplier 的包装器。调用 close() 会自动调用 release()，
   * 非常适合在 try-with-resources 块中使用，确保对象被正确释放。
   * The same as {@link #retain()} except that this method returns a {@link UncheckedAutoCloseableSupplier}.
   *
   * @return a {@link UncheckedAutoCloseableSupplier}
   *         where {@link java.util.function.Supplier#get()} will return the retained object,
   *         i.e. the object returned by {@link #retain()},
   *         and calling {@link UncheckedAutoCloseable#close()} one or more times
   *         is the same as calling {@link #release()} once (idempotent).
   */
  default UncheckedAutoCloseableSupplier<T> retainAndReleaseOnClose() {
    final T retained = retain();
    final AtomicBoolean closed = new AtomicBoolean();
    return new UncheckedAutoCloseableSupplier<T>() {
      @Override
      public T get() {
        if (closed.get()) {
          throw new IllegalStateException("Already closed");
        }
        return retained;
      }

      @Override
      public void close() {
        if (closed.compareAndSet(false, true)) {
          release();
        }
      }
    };
  }

  /**
   * 减少对象的引用计数。如果引用计数归零，返回 true，表示对象完全释放。
   * The reference count will be decreased by 1.
   *
   * @return true if the object is completely released (i.e. reference count becomes 0); otherwise, return false.
   */
  boolean release();

  /** The same as wrap(value, EMPTY, EMPTY), where EMPTY is an empty method. */
  static <V> ReferenceCountedObject<V> wrap(V value) {
    return wrap(value, () -> {}, ignored -> {});
  }

  /**
   * 包装一个新的对象，该对象的生命周期依赖于一组 {@link ReferenceCountedObject}
   */
  static <T, V> ReferenceCountedObject<V> delegateFrom(Collection<ReferenceCountedObject<T>> fromRefs, V value) {
    return new ReferenceCountedObject<V>() {
      @Override
      public V get() {
        return value;
      }

      @Override
      public V retain() {
        fromRefs.forEach(ReferenceCountedObject::retain);
        return value;
      }

      @Override
      public boolean release() {
        boolean allReleased = true;
        for (ReferenceCountedObject<T> ref : fromRefs) {
          if (!ref.release()) {
            allReleased = false;
          }
        }
        return allReleased;
      }
    };
  }

  /**
   * 创建一个新的 ReferenceCountedObject，对 retain 和 release 的调用委托给原始对象。
   * @return a {@link ReferenceCountedObject} of the given value by delegating to this object.
   */
  default <V> ReferenceCountedObject<V> delegate(V value) {
    final ReferenceCountedObject<T> delegated = this;
    return new ReferenceCountedObject<V>() {
      @Override
      public V get() {
        return value;
      }

      @Override
      public V retain() {
        delegated.retain();
        return value;
      }

      @Override
      public boolean release() {
        return delegated.release();
      }
    };
  }

  /**
   * 对当前对象应用一个函数，将结果包装为新的 {@link ReferenceCountedObject}。
   * @return a {@link ReferenceCountedObject} by apply the given function to this object.
   */
  default <V> ReferenceCountedObject<V> apply(Function<T, V> function) {
    return delegate(function.apply(get()));
  }

  /**
   * 简单地将对象包装为一个引用计数对象{@link ReferenceCountedObject}。
   * 允许自定义 retain 和 release 行为，通过 Runnable 或 Consumer<Boolean> 实现。
   * @param value the value being wrapped.
   * @param retainMethod a method to run when {@link #retain()} is invoked.
   * @param releaseMethod a method to run when {@link #release()} is invoked,
   *                      where the method takes a boolean which is the same as the one returned by {@link #release()}.
   * @param <V> The value type.
   * @return the wrapped reference-counted object.
   */
  static <V> ReferenceCountedObject<V> wrap(V value, Runnable retainMethod, Consumer<Boolean> releaseMethod) {
    Objects.requireNonNull(value, "value == null");
    Objects.requireNonNull(retainMethod, "retainMethod == null");
    Objects.requireNonNull(releaseMethod, "releaseMethod == null");

    return ReferenceCountedLeakDetector.getFactory().create(value, retainMethod, releaseMethod);
  }

  /** The same as wrap(value, retainMethod, ignored -> releaseMethod.run()). */
  static <V> ReferenceCountedObject<V> wrap(V value, Runnable retainMethod, Runnable releaseMethod) {
    return wrap(value, retainMethod, ignored -> releaseMethod.run());
  }
}
