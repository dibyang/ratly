
package net.xdob.ratly.util.function;

/** Consumer with a throws-clause. */
@FunctionalInterface
public interface CheckedConsumer<INPUT, THROWABLE extends Throwable> {
  /**
   * The same as {@link java.util.function.Consumer#accept(Object)}
   * except that this method is declared with a throws-clause.
   */
  void accept(INPUT input) throws THROWABLE;

  /** @return a {@link CheckedFunction} with {@link Void} return type. */
  static <INPUT, THROWABLE extends Throwable> CheckedFunction<INPUT, Void, THROWABLE> asCheckedFunction(
      CheckedConsumer<INPUT, THROWABLE> consumer) {
    return input -> {
      consumer.accept(input);
      return null;
    };
  }
}
