package net.xdob.ratly.protocol.exceptions;

/**
 * This exception indicates the failure of a stale-read.
 */
public class StaleReadException extends RaftException {
  public StaleReadException(String message) {
    super(message);
  }
}
