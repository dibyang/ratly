package net.xdob.ratly.protocol.exceptions;

import java.io.IOException;

public class RaftException extends IOException {
  private static final long serialVersionUID = 1L;

  public RaftException(String message) {
    super(message);
  }

  public RaftException(Throwable cause) {
    super(cause);
  }

  public RaftException(String message, Throwable cause) {
    super(message, cause);
  }
}
