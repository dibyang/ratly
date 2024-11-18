
package net.xdob.ratly.util.function;

import java.util.function.Supplier;

/** Supplier of {@link String}. */
@FunctionalInterface
public interface StringSupplier extends Supplier<String> {
  /**
   * @return a {@link StringSupplier} which uses the given {@link Supplier}
   *         to override both {@link Supplier#get()} and {@link Object#toString()}.
   */
  static StringSupplier get(Supplier<String> supplier) {
    return new StringSupplier() {
      @Override
      public String get() {
        return supplier.get();
      }

      @Override
      public String toString() {
        return supplier.get();
      }
    };
  }
}
