package net.xdob.ratly.retry;

import net.xdob.ratly.util.TimeDuration;

/**
 * 重试策略的合集
 */
public interface RetryPolicies {
  /**
   * 无限重试，重试前不睡眠
   */
  static RetryPolicy retryForeverNoSleep() {
    return Constants.RETRY_FOREVER_NO_SLEEP;
  }

  /**
   * 不重试策略
   */
  static RetryPolicy noRetry() {
    return Constants.NO_RETRY;
  }

  /**
   * 无限重试，重试前睡眠指定时间
   * @param sleepTime 睡眠时间
   */
  static RetryForeverWithSleep retryForeverWithSleep(TimeDuration sleepTime) {
    return new RetryForeverWithSleep(sleepTime);
  }

  /**
   * 限定最大重试次数，重试前睡眠指定时间
   * @param maxAttempts 最大重试次数
   * @param sleepTime 睡眠时间
   */
  static RetryLimited retryUpToMaximumCountWithFixedSleep(int maxAttempts, TimeDuration sleepTime) {
    return new RetryLimited(maxAttempts, sleepTime);
  }

  class Constants {
    private static final RetryForeverNoSleep RETRY_FOREVER_NO_SLEEP = new RetryForeverNoSleep();
    private static final NoRetry NO_RETRY = new NoRetry();
  }

}
