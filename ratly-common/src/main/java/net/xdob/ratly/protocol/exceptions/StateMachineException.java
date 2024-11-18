
package net.xdob.ratly.protocol.exceptions;

import net.xdob.ratly.protocol.RaftGroupMemberId;

public class StateMachineException extends RaftException {
  private final boolean leaderShouldStepDown;

  public StateMachineException(RaftGroupMemberId serverId, Throwable cause) {
    // cause.getMessage is added to this exception message as the exception received through
    // RPC call contains similar message but Simulated RPC doesn't. Adding the message
    // from cause to this exception makes it consistent across simulated and other RPC implementations.
    super(cause.getClass().getName() + " from Server " + serverId + ": " + cause.getMessage(), cause);
    this.leaderShouldStepDown = true;
  }

  public StateMachineException(String msg) {
    super(msg);
    this.leaderShouldStepDown = true;
  }

  public StateMachineException(String message, Throwable cause) {
    super(message, cause);
    this.leaderShouldStepDown = true;
  }

  public StateMachineException(RaftGroupMemberId serverId, Throwable cause, boolean leaderShouldStepDown) {
    // cause.getMessage is added to this exception message as the exception received through
    // RPC call contains similar message but Simulated RPC doesn't. Adding the message
    // from cause to this exception makes it consistent across simulated and other RPC implementations.
    super(cause.getClass().getName() + " from Server " + serverId + ": " + cause.getMessage(), cause);
    this.leaderShouldStepDown = leaderShouldStepDown;
  }

  public StateMachineException(String msg, boolean leaderShouldStepDown) {
    super(msg);
    this.leaderShouldStepDown = leaderShouldStepDown;
  }

  public StateMachineException(String message, Throwable cause, boolean leaderShouldStepDown) {
    super(message, cause);
    this.leaderShouldStepDown = leaderShouldStepDown;
  }

  public boolean leaderShouldStepDown() {
    return leaderShouldStepDown;
  }
}
