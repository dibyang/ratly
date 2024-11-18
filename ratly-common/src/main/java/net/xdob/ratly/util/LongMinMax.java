package net.xdob.ratly.util;

/**
 * Min and max values in long.
 *
 * This class is mutable.
 * This class is NOT thread safe.
 */
public class LongMinMax {
  private long min;
  private long max;
  private boolean initialized = false;

  /** @return the min */
  public long getMin() {
    Preconditions.assertTrue(initialized, "This LongMinMax object is uninitialized.");
    return min;
  }

  /** @return the max */
  public long getMax() {
    Preconditions.assertTrue(initialized, "This LongMinMax object is uninitialized.");
    return max;
  }

  public boolean isInitialized() {
    return initialized;
  }

  /** Update min and max with the given number. */
  public void accumulate(long n) {
    if (!initialized) {
      min = max = n;
      initialized = true;
    } else if (n < min) {
      min = n;
    } else if (n > max) {
      max = n;
    }
  }

  /** Combine that to this. */
  public void combine(LongMinMax that) {
    if (that.initialized) {
      if (!this.initialized) {
        this.min = that.min;
        this.max = that.max;
        this.initialized = true;
      } else {
        if (that.min < this.min) {
          this.min = that.min;
        }
        if (that.max > this.max) {
          this.max = that.max;
        }
      }
    }
  }
}
