package net.xdob.ratly.metrics;

import net.xdob.ratly.util.UncheckedAutoCloseable;

import java.util.Optional;

/**
 * Timekeeper接口用于测量代码执行时间。
 * 提供了一个静态方法start用于启动计时，并返回一个UncheckedAutoCloseable对象，
 * 该对象在关闭时会自动停止计时。
 */
@FunctionalInterface
public interface Timekeeper {
  /**
   * NOOP是一个空操作的UncheckedAutoCloseable实例，用于在timekeeper为null时返回。
   */
  UncheckedAutoCloseable NOOP = () -> {};

  /**
   * 启动计时的方法。
   * 如果timekeeper不为null，则调用其time方法获取Context对象，并将其转换为UncheckedAutoCloseable对象。
   * 如果timekeeper为null，则返回NOOP。
   *
   * @param timekeeper Timekeeper实例
   * @return UncheckedAutoCloseable对象，用于停止计时
   */
  static UncheckedAutoCloseable start(Timekeeper timekeeper) {
    return Optional.ofNullable(timekeeper)
        .map(Timekeeper::time)
        .map(Context::toAutoCloseable)
        .orElse(NOOP);
  }

  /**
   * Context接口用于表示计时上下文。
   * 包含一个stop方法用于停止计时，并返回经过的时间。
   */
  @FunctionalInterface
  interface Context {
    /**
     * 停止计时的方法。
     * 返回自计时开始以来经过的时间。
     *
     * @return 经过的时间
     */
    long stop();

    /**
     * 将Context对象转换为UncheckedAutoCloseable对象的方法。
     * 返回一个UncheckedAutoCloseable对象，其close方法会调用stop方法。
     *
     * @return UncheckedAutoCloseable对象
     */
    default UncheckedAutoCloseable toAutoCloseable() {
      return this::stop;
    }
  }

  /**
   * 启动计时的方法。
   * 返回一个Context对象，该对象可以用于停止计时。
   *
   * @return Context对象
   */
  Context time();
}