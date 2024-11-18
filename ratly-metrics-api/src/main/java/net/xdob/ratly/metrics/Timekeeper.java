
package net.xdob.ratly.metrics;

import net.xdob.ratly.util.UncheckedAutoCloseable;

import java.util.Optional;

@FunctionalInterface
public interface Timekeeper {
  UncheckedAutoCloseable NOOP = () -> {};

  static UncheckedAutoCloseable start(Timekeeper timekeeper) {
    return Optional.ofNullable(timekeeper)
        .map(Timekeeper::time)
        .map(Context::toAutoCloseable)
        .orElse(NOOP);
  }

  @FunctionalInterface
  interface Context {
    long stop();

    default UncheckedAutoCloseable toAutoCloseable() {
      return this::stop;
    }
  }

  Context time();
}
