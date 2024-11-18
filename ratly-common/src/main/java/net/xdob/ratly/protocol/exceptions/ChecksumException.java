
package net.xdob.ratly.protocol.exceptions;

import java.io.IOException;

/** Thrown for checksum errors. */
public class ChecksumException extends IOException {
  private static final long serialVersionUID = 1L;
  private long pos;
  public ChecksumException(String description, long pos) {
    super(description);
    this.pos = pos;
  }

  public long getPos() {
    return pos;
  }
}
