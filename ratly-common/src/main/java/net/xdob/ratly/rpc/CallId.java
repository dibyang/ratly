package net.xdob.ratly.rpc;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A long ID for RPC calls.
 * <p>
 * This class is threadsafe.
 */
public final class CallId {
  private static final AtomicLong CALL_ID_COUNTER = new AtomicLong(1);

  private static final Comparator<Long> COMPARATOR = (left, right) -> {
    final long diff = left - right;
    // check diff < Long.MAX_VALUE/2 for the possibility of numerical overflow
    return diff == 0? 0: diff > 0 && diff < Long.MAX_VALUE/2? 1: -1;
  };

  /** @return a long comparator, which takes care the possibility of numerical overflow, for comparing call ids. */
  public static Comparator<Long> getComparator() {
    return COMPARATOR;
  }

  /** @return the default value. */
  public static long getDefault() {
    return 0;
  }

  /** @return the current value. */
  public static long get() {
    return CALL_ID_COUNTER.get() & Long.MAX_VALUE;
  }

  /** @return the current value and then increment. */
  public static long getAndIncrement() {
    return CALL_ID_COUNTER.getAndIncrement() & Long.MAX_VALUE;
  }

  private CallId() {}
}