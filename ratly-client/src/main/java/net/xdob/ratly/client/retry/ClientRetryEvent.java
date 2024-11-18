
package net.xdob.ratly.client.retry;

import net.xdob.ratly.protocol.RaftClientRequest;
import net.xdob.ratly.retry.RetryPolicy;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.TimeDuration;
import net.xdob.ratly.util.Timestamp;

/** An {@link RetryPolicy.Event} specific to client request failure. */
public class ClientRetryEvent implements RetryPolicy.Event {
  private final int attemptCount;
  private final int causeCount;
  private final RaftClientRequest request;
  private final Throwable cause;
  private final Timestamp pendingRequestCreationTime;

  public ClientRetryEvent(int attemptCount, RaftClientRequest request, int causeCount, Throwable cause,
      Timestamp pendingRequestCreationTime) {
    this.attemptCount = attemptCount;
    this.causeCount = causeCount;
    this.request = request;
    this.cause = cause;
    this.pendingRequestCreationTime = pendingRequestCreationTime;
  }

  @Override
  public int getAttemptCount() {
    return attemptCount;
  }

  @Override
  public int getCauseCount() {
    return causeCount;
  }

  public RaftClientRequest getRequest() {
    return request;
  }

  @Override
  public Throwable getCause() {
    return cause;
  }

  boolean isRequestTimeout(TimeDuration timeout) {
    return timeout != null && pendingRequestCreationTime.elapsedTime().compareTo(timeout) >= 0;
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass())
        + ":attempt=" + attemptCount
        + ",request=" + request
        + ",cause=" + cause
        + ",causeCount=" + causeCount;
  }
}
