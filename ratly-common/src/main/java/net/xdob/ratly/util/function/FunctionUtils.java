package net.xdob.ratly.util.function;

import java.util.function.Consumer;
import java.util.function.Function;

public interface FunctionUtils {
  /**
   * Convert the given consumer to a function with any output type
   * such that the returned function always returns null.
   */
  static <INPUT, OUTPUT> Function<INPUT, OUTPUT> consumerAsNullFunction(Consumer<INPUT> consumer) {
    return input -> {
      consumer.accept(input);
      return null;
    };
  }
}
