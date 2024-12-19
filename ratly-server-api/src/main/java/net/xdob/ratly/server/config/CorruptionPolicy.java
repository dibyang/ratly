package net.xdob.ratly.server.config;

import java.util.Optional;
import java.util.function.Function;

/**
 * 日志损坏策略
 * 日志损坏的处理策略
 */
public enum CorruptionPolicy {
  /**
   * Rethrow the exception.
   */
  EXCEPTION,
  /**
   * Print a warn log message and return all uncorrupted log entries up to the corruption.
   */
  WARN_AND_RETURN;

  public static CorruptionPolicy getDefault() {
    return EXCEPTION;
  }

  public static <T> CorruptionPolicy get(T supplier, Function<T, CorruptionPolicy> getMethod) {
    return Optional.ofNullable(supplier).map(getMethod).orElse(getDefault());
  }
}
