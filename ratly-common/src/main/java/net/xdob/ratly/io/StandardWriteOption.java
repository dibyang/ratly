
package net.xdob.ratly.io;

public enum StandardWriteOption implements WriteOption {
  /**
   * Sync the data to the underlying storage.
   * Note that SYNC does not imply {@link #FLUSH}.
   */
  SYNC,
  /** Close the data to the underlying storage. */
  CLOSE,
  /** Flush the data out from the buffer. */
  FLUSH,
}
