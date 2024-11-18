package net.xdob.ratly.protocol.exceptions;

public class ReconfigurationTimeoutException extends RaftException {
  public ReconfigurationTimeoutException(String message) {
    super(message);
  }
}
