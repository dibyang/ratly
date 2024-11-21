package net.xdob.ratly.protocol.exceptions;


/**
 * This indicates a retryable read exception
 */
public class ReadIndexException extends RaftException {

  public ReadIndexException(String message) {
    super(message);
  }
  public ReadIndexException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
