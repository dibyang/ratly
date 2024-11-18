
package net.xdob.ratly.protocol.exceptions;

import net.xdob.ratly.protocol.RaftClientRequest;
import net.xdob.ratly.retry.RetryPolicy;

/**
 * Retry failure as per the {@link RetryPolicy} defined.
 */
public class RaftRetryFailureException extends RaftException {

  private final int attemptCount;

  public RaftRetryFailureException(
      RaftClientRequest request, int attemptCount, RetryPolicy retryPolicy, Throwable cause) {
    super("Failed " + request + " for " + attemptCount + " attempts with " + retryPolicy, cause);
    this.attemptCount = attemptCount;
  }

  public int getAttemptCount() {
    return attemptCount;
  }
}