
package net.xdob.ratly.examples.counter;

import net.xdob.ratly.protocol.Message;
import com.google.protobuf.ByteString;

/**
 * The supported commands the Counter example.
 */
public enum CounterCommand {
  /** Increment the counter by 1. */
  INCREMENT,
  /** Query the counter value. */
  GET;

  private final Message message = Message.valueOf(name());

  public Message getMessage() {
    return message;
  }

  /** Does the given command string match this command? */
  public boolean matches(String command) {
    return name().equalsIgnoreCase(command);
  }

  /** Does the given command string match this command? */
  public boolean matches(ByteString command) {
    return message.getContent().equals(command);
  }
}
