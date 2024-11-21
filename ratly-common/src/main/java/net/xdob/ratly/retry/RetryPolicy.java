package net.xdob.ratly.retry;

import net.xdob.ratly.util.TimeDuration;

/**
 * 重试策略接口
 */
@FunctionalInterface
public interface RetryPolicy {
  Action NO_RETRY_ACTION = new Action() {
    @Override
    public boolean shouldRetry() {
      return false;
    }
    @Override
    public TimeDuration getSleepTime() {
      return TimeDuration.ZERO;
    }
  };

  Action RETRY_WITHOUT_SLEEP_ACTION = () -> TimeDuration.ZERO;

  /** The action it should take. */
  @FunctionalInterface
  interface Action {
    /** @return true if it has to make another attempt; otherwise, return false. */
    default boolean shouldRetry() {
      return true;
    }

    /** @return the sleep time period before the next attempt. */
    TimeDuration getSleepTime();
  }

  /** The event triggered the failure. */
  @FunctionalInterface
  interface Event {
    /** @return the number of attempts tried so far. */
    int getAttemptCount();

    /** @return the number of attempts for the event cause. */
    default int getCauseCount() {
      return 0;
    }

    default Throwable getCause() {
      return null;
    }
  }

  /**
   * Determines whether it is supposed to retry after the operation has failed.
   *
   * @param event The failed event.
   * @return the action it should take.
   */
  Action handleAttemptFailure(Event event);
}
