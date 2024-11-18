package net.xdob.ratly.protocol.exceptions;

import java.io.IOException;

/**
 * Signals that an attempt to create a file at a given pathname has failed
 * because another file already existed at that path.
 */
public class AlreadyExistsException extends IOException {
  private static final long serialVersionUID = 1L;

  public AlreadyExistsException(String msg) {
    super(msg);
  }

  public AlreadyExistsException(Throwable cause) {
    super(cause);
  }
}