
package net.xdob.ratly.protocol.exceptions;

import net.xdob.ratly.protocol.RaftPeerId;

public class DataStreamException extends RaftException {
  public DataStreamException(RaftPeerId peerId, Throwable cause) {
    super(cause.getClass().getName() + " from Server " + peerId + ": " + cause.getMessage(), cause);
  }

  public DataStreamException(String msg) {
    super(msg);
  }
}
