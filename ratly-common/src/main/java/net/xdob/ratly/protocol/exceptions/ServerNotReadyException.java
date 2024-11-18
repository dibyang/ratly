package net.xdob.ratly.protocol.exceptions;

/**
 * The server is not ready yet.
 */
public class ServerNotReadyException extends RaftException {
  public ServerNotReadyException(String message) {
    super(message);
  }
}
