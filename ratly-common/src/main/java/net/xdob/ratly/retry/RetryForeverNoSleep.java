package net.xdob.ratly.retry;

import net.xdob.ratly.util.JavaUtils;

/**
 * 无限重试，重试前不睡眠
 */
public final class RetryForeverNoSleep implements RetryPolicy {
  RetryForeverNoSleep() {
  }

  @Override
  public Action handleAttemptFailure(Event event) {
    return RetryPolicy.RETRY_WITHOUT_SLEEP_ACTION;
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass());
  }
}
