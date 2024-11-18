
package net.xdob.ratly.io;

import java.io.File;

/**
 * Encapsulate a {@link File} with a starting position and a byte count.
 *
 * The class is immutable.
 */
public final class FilePositionCount {
  public static FilePositionCount valueOf(File file, long position, long count) {
    return new FilePositionCount(file, position, count);
  }

  private final File file;
  private final long position;
  private final long count;

  private FilePositionCount(File file, long position, long count) {
    this.file = file;
    this.position = position;
    this.count = count;
  }

  /** @return the file. */
  public File getFile() {
    return file;
  }

  /** @return the starting position. */
  public long getPosition() {
    return position;
  }

  /** @return the byte count. */
  public long getCount() {
    return count;
  }
}
