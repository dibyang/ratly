package net.xdob.ratly.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

public class AutoCloseableReadWriteLock {
  private final Object name;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
  private final AtomicInteger depth = new AtomicInteger();

  public AutoCloseableReadWriteLock(Object name) {
    this.name = name;
  }

  public AutoCloseableLock readLock(StackTraceElement caller, Consumer<String> log) {
    final AutoCloseableLock readLock = AutoCloseableLock.acquire(lock.readLock(),
        () -> logLocking(caller, true, false, log));

    logLocking(caller, true, true, log);
    return readLock;
  }

  public AutoCloseableLock writeLock(StackTraceElement caller, Consumer<String> log) {
    final AutoCloseableLock writeLock = AutoCloseableLock.acquire(lock.writeLock(),
        () -> logLocking(caller, false, false, log));

    logLocking(caller, false, true, log);
    return writeLock;
  }

  private void logLocking(StackTraceElement caller, boolean read, boolean acquire, Consumer<String> log) {
    if (caller != null && log != null) {
      final int d = acquire? depth.getAndIncrement(): depth.decrementAndGet();
      final StringBuilder b = new StringBuilder();
      for(int i = 0; i < d; i++) {
        b.append("  ");
      }
      if (name != null) {
        b.append(name).append(": ");
      }
      b.append(read? "readLock ": "writeLock ")
          .append(acquire ? "ACQUIRED ": "RELEASED ")
          .append(depth).append(" by ");
      final String className = caller.getClassName();
      final int i = className.lastIndexOf('.');
      b.append(i >= 0? className.substring(i + 1): className).append(".").append(caller.getMethodName());
      log.accept(b.toString());
    }
  }
}
