
package net.xdob.ratly.util.function;

import net.xdob.ratly.util.TimeDuration;

import java.util.concurrent.TimeoutException;

/** Function with a timeout and a throws-clause. */
@FunctionalInterface
public interface CheckedFunctionWithTimeout<INPUT, OUTPUT, THROWABLE extends Throwable> {
  /**
   * The same as {@link CheckedFunction#apply(Object)}
   * except that this method has a timeout parameter and throws {@link TimeoutException}.
   */
  OUTPUT apply(INPUT input, TimeDuration timeout) throws TimeoutException, THROWABLE;
}
