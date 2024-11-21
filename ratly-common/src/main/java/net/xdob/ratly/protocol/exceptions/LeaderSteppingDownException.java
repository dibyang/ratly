package net.xdob.ratly.protocol.exceptions;

public class LeaderSteppingDownException extends RaftException {

  public LeaderSteppingDownException(String message) {
    super(message);
  }

  public LeaderSteppingDownException(String message, Throwable t) {
    super(message, t);
  }
}
