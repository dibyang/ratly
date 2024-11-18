package net.xdob.ratly.util;

import net.xdob.ratly.util.function.CheckedSupplier;

import java.util.Objects;

/**
 * A memoized supplier is a {@link CheckedSupplier}
 * which gets a value by invoking its initializer once.
 * and then keeps returning the same value as its supplied results.
 *
 * This class is thread safe.
 *
 * @param <RETURN> The return type of the supplier.
 * @param <THROW> The throwable type of the supplier.
 */
public final class MemoizedCheckedSupplier<RETURN, THROW extends Throwable>
    implements CheckedSupplier<RETURN, THROW> {
  /**
   * @param supplier to supply at most one non-null value.
   * @return a {@link MemoizedCheckedSupplier} with the given supplier.
   */
  public static <RETURN, THROW extends Throwable> MemoizedCheckedSupplier<RETURN, THROW> valueOf(
      CheckedSupplier<RETURN, THROW> supplier) {
    return supplier instanceof MemoizedCheckedSupplier ?
        (MemoizedCheckedSupplier<RETURN, THROW>) supplier : new MemoizedCheckedSupplier<>(supplier);
  }

  private final CheckedSupplier<RETURN, THROW> initializer;

  @SuppressWarnings({"squid:S3077"}) // Suppress volatile for generic type
  private volatile RETURN value = null;

  /**
   * Create a memoized supplier.
   * @param initializer to supply at most one non-null value.
   */
  private MemoizedCheckedSupplier(CheckedSupplier<RETURN, THROW> initializer) {
    Objects.requireNonNull(initializer, "initializer == null");
    this.initializer = initializer;
  }

  /** @return the lazily initialized object. */
  @Override
  public RETURN get() throws THROW {
    RETURN v = value;
    if (v == null) {
      synchronized (this) {
        v = value;
        if (v == null) {
          v = value = Objects.requireNonNull(initializer.get(), "initializer.get() returns null");
        }
      }
    }
    return v;
  }

  /**
   * @return the already initialized object.
   * @throws NullPointerException if the object is uninitialized.
   */
  public RETURN getUnchecked() {
    return Objects.requireNonNull(value, "value == null");
  }

  /** @return is the object initialized? */
  public boolean isInitialized() {
    return value != null;
  }

  @Override
  public String toString() {
    return isInitialized()? "Memoized:" + value: "UNINITIALIZED";
  }
}
