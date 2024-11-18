
package net.xdob.ratly.util;

import net.xdob.ratly.util.function.CheckedRunnable;
import net.xdob.ratly.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * Logging (as in log4j) related utility methods.
 */
public interface LogUtils {
  Logger LOG = LoggerFactory.getLogger(LogUtils.class);

  static <THROWABLE extends Throwable> void runAndLog(
      Logger log, CheckedRunnable<THROWABLE> op, Supplier<String> opName)
      throws THROWABLE {
    try {
      op.run();
    } catch (Throwable t) {
      if (log.isTraceEnabled()) {
        log.trace("Failed to " + opName.get(), t);
      } else if (log.isWarnEnabled()){
        log.warn("Failed to " + opName.get() + ": " + t);
      }
      throw t;
    }

    if (log.isTraceEnabled()) {
      log.trace("Successfully ran " + opName.get());
    }
  }

  static <OUTPUT, THROWABLE extends Throwable> OUTPUT supplyAndLog(
      Logger log, CheckedSupplier<OUTPUT, THROWABLE> supplier, Supplier<String> name)
      throws THROWABLE {
    final OUTPUT output;
    try {
      output = supplier.get();
    } catch (Exception e) {
      if (log.isTraceEnabled()) {
        log.trace("Failed to " + name.get(), e);
      } else if (log.isWarnEnabled()){
        log.warn("Failed to " + name.get() + ": " + e);
      }
      throw e;
    }

    if (log.isTraceEnabled()) {
      log.trace("Successfully supplied " + name.get() + ": " + output);
    }
    return output;
  }

  static Runnable newRunnable(Logger log, Runnable runnable, Supplier<String> name) {
    return new Runnable() {
      @Override
      public void run() {
        runAndLog(log, runnable::run, name);
      }

      @Override
      public String toString() {
        return name.get();
      }
    };
  }

  static <T> Callable<T> newCallable(Logger log, Callable<T> callable, Supplier<String> name) {
    return new Callable<T>() {
      @Override
      public T call() throws Exception {
        return supplyAndLog(log, callable::call, name);
      }

      @Override
      public String toString() {
        return name.get();
      }
    };
  }

  static <OUTPUT, THROWABLE extends Throwable> CheckedSupplier<OUTPUT, THROWABLE> newCheckedSupplier(
      Logger log, CheckedSupplier<OUTPUT, THROWABLE> supplier, Supplier<String> name) {
    return new CheckedSupplier<OUTPUT, THROWABLE>() {
      @Override
      public OUTPUT get() throws THROWABLE {
        return supplyAndLog(log, supplier, name);
      }

      @Override
      public String toString() {
        return name.get();
      }
    };
  }

  static void warn(Logger log, Supplier<String> message, Throwable t, Class<?>... exceptionClasses) {
    if (log.isWarnEnabled()) {
      if (ReflectionUtils.isInstance(t, exceptionClasses)) {
        // do not print stack trace for known exceptions.
        log.warn(message.get() + ": " + t);
      } else {
        log.warn(message.get(), t);
      }
    }
  }

  static void infoOrTrace(Logger log, String message, Throwable t) {
    infoOrTrace(log, () -> message, t);
  }

  static void infoOrTrace(Logger log, Supplier<String> message, Throwable t) {
    if (log.isTraceEnabled()) {
      log.trace(message.get(), t);
    } else if (log.isInfoEnabled()) {
      log.info("{}: {}", message.get(), String.valueOf(t));
    }
  }
}
