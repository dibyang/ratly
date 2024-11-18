
package net.xdob.ratly.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Use a {@link Daemon},
 * which repeatedly waits for a signal to run a method.
 * <p>
 * This class is threadsafe.
 *
 * @see AwaitForSignal
 */
public class AwaitToRun implements AutoCloseable {
  public static final Logger LOG = LoggerFactory.getLogger(AwaitToRun.class);

  private final class RunnableImpl implements Runnable {
    private final Runnable runMethod;

    private RunnableImpl(Runnable runMethod) {
      this.runMethod = runMethod;
    }

    @Override
    public void run() {
      while(!Thread.currentThread().isInterrupted()) {
        try {
          awaitForSignal.await();
        } catch (InterruptedException e) {
          LOG.info("{} is interrupted", awaitForSignal);
          Thread.currentThread().interrupt();
          return;
        }

        try {
          runMethod.run();
        } catch (Throwable t) {
          LOG.error(name + ": runMethod failed", t);
        }
      }
    }
  }

  private final String name;
  private final AwaitForSignal awaitForSignal;
  private final AtomicReference<Daemon> daemon;

  public AwaitToRun(Object namePrefix, Runnable runMethod) {
    this.name = namePrefix + "-" + JavaUtils.getClassSimpleName(getClass());
    this.awaitForSignal = new AwaitForSignal(name);
    this.daemon = new AtomicReference<>(Daemon.newBuilder()
        .setName(name)
        .setRunnable(new RunnableImpl(runMethod))
        .build());
  }

  /** Similar to {@link Thread#start()}. */
  public AwaitToRun start() {
    final Daemon d = daemon.get();
    if (d != null) {
      d.start();
      LOG.info("{} started", d);
    } else {
      LOG.warn("{} is already closed", name);
    }
    return this;
  }

  /** Signal to run. */
  public void signal() {
    awaitForSignal.signal();
  }

  @Override
  public void close() {
    final Daemon d = daemon.getAndSet(null);
    if (d == null) {
      return;
    }

    d.interrupt();
    try {
      d.join();
    } catch (InterruptedException e) {
      LOG.warn(d + ": join is interrupted", e);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public String toString() {
    return name;
  }
}
