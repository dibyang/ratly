package net.xdob.ratly.io;

import java.io.File;

/**
 * 封装一个对象，标识文件、起始位置和数据大小(字节数)
 * <p>
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
