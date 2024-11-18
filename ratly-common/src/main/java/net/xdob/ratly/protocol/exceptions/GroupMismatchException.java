package net.xdob.ratly.protocol.exceptions;

/**
 * This exception indicates that the group id in the request does not match
 * server's group id.
 */
public class GroupMismatchException extends RaftException {
  public GroupMismatchException(String message) {
    super(message);
  }
}
