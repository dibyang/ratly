
package net.xdob.ratly.metrics;

public interface LongCounter {
  default void inc() {
    inc(1L);
  }

  void inc(long n);

  default void dec() {
    dec(1L);
  }

  void dec(long n);

  long getCount();
}
