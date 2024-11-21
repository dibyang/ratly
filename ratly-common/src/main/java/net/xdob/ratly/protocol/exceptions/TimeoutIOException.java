package net.xdob.ratly.protocol.exceptions;

import java.io.IOException;

/**
 * Timeout has occurred for a blocking I/O.
 */
public class TimeoutIOException extends IOException {
  static final long serialVersionUID = 1L;

  public TimeoutIOException(String message) {
    super(message);
  }

  public TimeoutIOException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
