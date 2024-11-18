package net.xdob.ratly.protocol.exceptions;

/**
 * This exception indicates the failure of a read request.
 */
public class ReadException extends RaftException {
  public ReadException(String message) {
    super(message);
  }

  public ReadException(Throwable cause) {
    super(cause);
  }
}
