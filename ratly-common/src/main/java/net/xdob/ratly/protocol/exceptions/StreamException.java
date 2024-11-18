
package net.xdob.ratly.protocol.exceptions;

/**
 * A stream is not found in the server.
 */
public class StreamException extends RaftException {
  public StreamException(String message) {
    super(message);
  }
}
