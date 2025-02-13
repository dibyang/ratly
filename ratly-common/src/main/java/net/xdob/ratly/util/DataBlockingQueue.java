package net.xdob.ratly.util;

import net.xdob.ratly.util.function.CheckedFunctionWithTimeout;
import net.xdob.ratly.util.function.TriConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;

/**
 * A queue for data elements
 * such that the queue imposes limits on both number of elements and the data size in bytes.
 *
 * Null element is NOT supported.
 *
 * This class is threadsafe.
 */
public class DataBlockingQueue<E> extends DataQueue<E> {
  public static final Logger LOG = LoggerFactory.getLogger(DataBlockingQueue.class);

  private final Lock lock = new ReentrantLock();
  private final Condition notFull  = lock.newCondition();
  private final Condition notEmpty = lock.newCondition();

  private boolean closed = false;

  public DataBlockingQueue(Object name, SizeInBytes byteLimit, int elementLimit, ToLongFunction<E> getNumBytes) {
    super(name, byteLimit, elementLimit, getNumBytes);
  }

  @Override
  public long getNumBytes() {
    try(AutoCloseableLock auto = AutoCloseableLock.acquire(lock)) {
      return super.getNumBytes();
    }
  }

  @Override
  public int getNumElements() {
    try(AutoCloseableLock auto = AutoCloseableLock.acquire(lock)) {
      return super.getNumElements();
    }
  }

  @Override
  public void clear() {
    try(AutoCloseableLock auto = AutoCloseableLock.acquire(lock)) {
      super.clear();
      notFull.signal();
    }
  }

  /** Apply the given handler to each element and then {@link #clear()}. */
  public void clear(Consumer<E> handler) {
    try(AutoCloseableLock auto = AutoCloseableLock.acquire(lock)) {
      for(E e : this) {
        handler.accept(e);
      }
      super.clear();
    }
  }

  /**
   * Close this queue to stop accepting new elements, i.e. the offer(…) methods always return false.
   * Note that closing the queue will not clear the existing elements.
   * The existing elements can be peeked, polled or cleared after close.
   */
  public void close() {
    try(AutoCloseableLock ignored = AutoCloseableLock.acquire(lock)) {
      closed = true;
    }
  }

  @Override
  public boolean offer(E element) {
    Objects.requireNonNull(element, "element == null");
    try(AutoCloseableLock auto = AutoCloseableLock.acquire(lock)) {
      if (closed) {
        return false;
      }
      if (super.offer(element)) {
        notEmpty.signal();
        return true;
      }
      return false;
    }
  }

  /**
   * Adds an element to this queue, waiting up to the given timeout.
   *
   * @return true if the element is added successfully;
   *         otherwise, the element is not added, return false.
   */
  public boolean offer(E element, TimeDuration timeout) throws InterruptedException {
    Objects.requireNonNull(element, "element == null");
    long nanos = timeout.toLong(TimeUnit.NANOSECONDS);
    try(AutoCloseableLock auto = AutoCloseableLock.acquire(lock)) {
      for(;;) {
        if (closed) {
          return false;
        }
        if (super.offer(element)) {
          notEmpty.signal();
          return true;
        }
        if (nanos <= 0) {
          return false;
        }
        nanos = notFull.awaitNanos(nanos);
      }

    }
  }

  @Override
  public boolean remove(E e) {
    try(AutoCloseableLock auto = AutoCloseableLock.acquire(lock)) {
      final boolean removed = super.remove(e);
      if (removed) {
        notFull.signal();
      }
      return removed;
    }
  }

  @Override
  public E poll() {
    try(AutoCloseableLock auto = AutoCloseableLock.acquire(lock)) {
      final E polled = super.poll();
      if (polled != null) {
        notFull.signal();
      }
      return polled;
    }
  }

  /**
   * Poll out the head element from this queue, waiting up to the given timeout.
   */
  public E poll(TimeDuration timeout) throws InterruptedException {
    long nanos = timeout.toLong(TimeUnit.NANOSECONDS);
    try(AutoCloseableLock auto = AutoCloseableLock.acquire(lock)) {
      for(;;) {
        final E polled = super.poll();
        if (polled != null) {
          notFull.signal();
          return polled;
        }
        if (nanos <= 0) {
          return null;
        }
        nanos = notEmpty.awaitNanos(nanos);
      }
    }
  }

  @Override
  public <RESULT, THROWABLE extends Throwable> List<RESULT> pollList(long timeoutMs,
      CheckedFunctionWithTimeout<E, RESULT, THROWABLE> getResult,
      TriConsumer<E, TimeDuration, TimeoutException> timeoutHandler) throws THROWABLE {
    try(AutoCloseableLock auto = AutoCloseableLock.acquire(lock)) {
      final List<RESULT> results = super.pollList(timeoutMs, getResult, timeoutHandler);
      if (!results.isEmpty()) {
        notFull.signal();
      }
      return results;
    }
  }

  @Override
  public E peek() {
    try(AutoCloseableLock auto = AutoCloseableLock.acquire(lock)) {
      return super.peek();
    }
  }
}
