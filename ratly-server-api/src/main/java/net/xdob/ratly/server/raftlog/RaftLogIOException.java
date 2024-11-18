
package net.xdob.ratly.server.raftlog;

import net.xdob.ratly.protocol.exceptions.RaftException;

/**
 * Exception while reading/writing RaftLog
 */
public class RaftLogIOException extends RaftException {
  public RaftLogIOException(Throwable cause) {
    super(cause);
  }

  public RaftLogIOException(String msg) {
    super(msg);
  }

  public RaftLogIOException(String message, Throwable cause) {
    super(message, cause);
  }
}
