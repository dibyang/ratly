package net.xdob.ratly.retry;

import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.TimeDuration;

/**
 * 无限重试，在两次尝试之间保持固定的睡眠时间。
 */
public class RetryForeverWithSleep implements RetryPolicy {
  private final TimeDuration sleepTime;

  /**
   *
   * @param sleepTime 睡眠时间
   */
  RetryForeverWithSleep(TimeDuration sleepTime) {
    Preconditions.assertTrue(!sleepTime.isNegative(), () -> "sleepTime = " + sleepTime + " < 0");
    this.sleepTime = sleepTime;
  }

  @Override
  public Action handleAttemptFailure(Event event) {
    return () -> sleepTime;
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass()) + "(sleepTime = " + sleepTime + ")";
  }
}
