
package net.xdob.ratly.protocol.exceptions;

public class SetConfigurationException extends RaftException {

  public SetConfigurationException(String message) {
    super(message);
  }

  public SetConfigurationException(String message, Throwable t) {
    super(message, t);
  }
}
