package net.xdob.ratly.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is a partial implementation of {@link Condition}.
 * Only some of the await and signal methods are implemented.
 * <p>
 * This class is threadsafe.
 */
public class AwaitForSignal {
  private final String name;
  private final Lock lock = new ReentrantLock();
  private final Condition condition = lock.newCondition();
  private final AtomicReference<AtomicBoolean> signaled = new AtomicReference<>(new AtomicBoolean());

  public AwaitForSignal(Object name) {
    this.name = name + "-" + JavaUtils.getClassSimpleName(getClass());
  }

  /** The same as {@link Condition#await()} */
  public void await() throws InterruptedException {
    lock.lock();
    try {
      for (final AtomicBoolean s = signaled.get(); !s.get(); ) {
        condition.await();
      }
    } finally {
      lock.unlock();
    }
  }

  /** The same as {@link Condition#await(long, TimeUnit)} */
  public boolean await(long time, TimeUnit unit) throws InterruptedException {
    if (time <= 0) {
      return false;
    }
    lock.lock();
    try {
      if (signaled.get().get()) {
        return true;
      }
      return condition.await(time, unit);
    } finally {
      lock.unlock();
    }
  }

  /** The same as {@link Condition#signal()} */
  public void signal() {
    lock.lock();
    try {
      signaled.getAndSet(new AtomicBoolean()).set(true);
      condition.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public String toString() {
    return name;
  }
}