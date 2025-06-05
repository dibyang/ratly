package net.xdob.ratly.util;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A memoized supplier is a {@link Supplier}
 * which gets a value by invoking its initializer once
 * and then keeps returning the same value as its supplied results.
 *
 * This class is thread safe.
 *
 * @param <T> The supplier result type.
 */
public final class MemoizedSupplier<T> implements Supplier<T> {
  /**
   * @param supplier to supply at most one non-null value.
   * @return a {@link MemoizedSupplier} with the given supplier.
   */
  public static <T> MemoizedSupplier<T> valueOf(Supplier<T> supplier) {
    return supplier instanceof MemoizedSupplier ?
        (MemoizedSupplier<T>) supplier : new MemoizedSupplier<>(supplier);
  }

  private final Supplier<T> initializer;
  @SuppressWarnings({"squid:S3077"}) // Suppress volatile for generic type
  private volatile T value = null;

  /**
   * Create a memoized supplier.
   * @param initializer to supply at most one non-null value.
   */
  private MemoizedSupplier(Supplier<T> initializer) {
    Objects.requireNonNull(initializer, "initializer == null");
    this.initializer = initializer;
  }

  /** @return the lazily initialized object. */
  @Override
  public T get() {
    T v = value;
    if (v == null) {
      synchronized (this) {
        v = value;
        if (v == null) {
          v = value = Objects.requireNonNull(initializer.get(),
              "initializer.get() returns null");
        }
      }
    }
    return v;
  }

  /** @return is the object initialized? */
  public boolean isInitialized() {
    return value != null;
  }

  public Optional<T> release() {
    T v = value;
    value = null;
    return Optional.ofNullable(v);
  }

  @Override
  public String toString() {
    return isInitialized()? "Memoized:" + get(): "UNINITIALIZED";
  }
}
