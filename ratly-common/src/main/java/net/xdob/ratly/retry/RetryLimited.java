package net.xdob.ratly.retry;

import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.TimeDuration;

import java.util.function.Supplier;

/**
 * 限定最大重试次数，在两次尝试之间保持固定的睡眠时间。
 */
public final class RetryLimited extends RetryForeverWithSleep {
  private final int maxAttempts;
  private final Supplier<String> myString;

  /**
   *
   * @param maxAttempts 最大重试次数
   * @param sleepTime 睡眠时间
   */
  RetryLimited(int maxAttempts, TimeDuration sleepTime) {
    super(sleepTime);

    if (maxAttempts < 0) {
      throw new IllegalArgumentException("maxAttempts = " + maxAttempts + " < 0");
    }

    this.maxAttempts = maxAttempts;
    this.myString = JavaUtils.memoize(() -> JavaUtils.getClassSimpleName(getClass())
        + "(maxAttempts=" + maxAttempts + ", sleepTime=" + sleepTime + ")");
  }

  public int getMaxAttempts() {
    return maxAttempts;
  }

  @Override
  public Action handleAttemptFailure(Event event) {
    return event.getAttemptCount() < maxAttempts ? super.handleAttemptFailure(event) : NO_RETRY_ACTION;
  }

  @Override
  public String toString() {
    return myString.get();
  }
}
