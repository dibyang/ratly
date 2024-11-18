
package net.xdob.ratly.util;

import net.xdob.ratly.util.function.CheckedRunnable;
import org.slf4j.Logger;

import java.util.function.Consumer;
import java.util.function.Supplier;

/** Execute timeout tasks. */
public interface TimeoutExecutor {
  int MAXIMUM_POOL_SIZE =  8;
  static TimeoutExecutor getInstance() {
    return TimeoutTimer.getInstance();
  }

  /** @return the number of scheduled but not completed timeout tasks. */
  int getTaskCount();

  /**
   * Schedule a timeout task.
   *
   * @param timeout the timeout value.
   * @param task the task to run when timeout.
   * @param errorHandler to handle the error, if there is any.
   */
  <THROWABLE extends Throwable> void onTimeout(
      TimeDuration timeout, CheckedRunnable<THROWABLE> task, Consumer<THROWABLE> errorHandler);

  /** When timeout, run the task.  Log the error, if there is any. */
  default void onTimeout(TimeDuration timeout, CheckedRunnable<?> task, Logger log, Supplier<String> errorMessage) {
    onTimeout(timeout, task, t -> log.error(errorMessage.get(), t));
  }
}
