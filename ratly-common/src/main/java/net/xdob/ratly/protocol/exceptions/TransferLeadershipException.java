package net.xdob.ratly.protocol.exceptions;

public class TransferLeadershipException extends RaftException {

  public TransferLeadershipException(String message) {
    super(message);
  }

  public TransferLeadershipException(String message, Throwable t) {
    super(message, t);
  }
}
