
package net.xdob.ratly.client.retry;

import net.xdob.ratly.proto.raft.*;
import net.xdob.ratly.retry.RetryPolicies;
import net.xdob.ratly.retry.RetryPolicy;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.TimeDuration;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A {@link net.xdob.ratly.protocol.RaftClientRequest.Type} dependent {@link RetryPolicy}
 * such that each type can be set to use an individual policy.
 * When the policy is not set for a particular type,
 * the {@link RetryPolicies#retryForeverNoSleep()} policy is used as the default.
 */
public final class RequestTypeDependentRetryPolicy implements RetryPolicy {
  public static class Builder {
    private final EnumMap<RaftClientRequestProto.TypeCase, RetryPolicy>
        retryPolicyMap = new EnumMap<>(RaftClientRequestProto.TypeCase.class);
    private EnumMap<RaftClientRequestProto.TypeCase, TimeDuration>
        timeoutMap = new EnumMap<>(RaftClientRequestProto.TypeCase.class);

    /** Set the given policy for the given type. */
    public Builder setRetryPolicy(RaftClientRequestProto.TypeCase type, RetryPolicy policy) {
      final RetryPolicy previous = retryPolicyMap.put(type, policy);
      Preconditions.assertNull(previous, () -> "The retryPolicy for type " + type + " is already set to " + previous);
      return this;
    }

    public Builder setTimeout(RaftClientRequestProto.TypeCase type, TimeDuration timeout) {
      final TimeDuration previous = timeoutMap.put(type, timeout);
      Preconditions.assertNull(previous, () -> "The timeout for type " + type + " is already set to " + previous);
      return this;
    }

    public RequestTypeDependentRetryPolicy build() {
      return new RequestTypeDependentRetryPolicy(retryPolicyMap, timeoutMap);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final Map<RaftClientRequestProto.TypeCase, RetryPolicy> retryPolicyMap;
  private final Map<RaftClientRequestProto.TypeCase, TimeDuration> timeoutMap;
  private final Supplier<String> myString;

  private RequestTypeDependentRetryPolicy(
      EnumMap<RaftClientRequestProto.TypeCase, RetryPolicy> map,
      EnumMap<RaftClientRequestProto.TypeCase, TimeDuration> timeoutMap) {
    this.retryPolicyMap = Collections.unmodifiableMap(map);
    this.timeoutMap = timeoutMap;
    this.myString = () -> {
      final StringBuilder b = new StringBuilder(JavaUtils.getClassSimpleName(getClass())).append("{");
      map.forEach((key, value) -> b.append(key).append("->").append(value).append(", "));
      b.setLength(b.length() - 2);
      return b.append("}").toString();
    };
  }

  @Override
  public Action handleAttemptFailure(Event event) {
    if (!(event instanceof ClientRetryEvent)) {
      return RetryPolicies.retryForeverNoSleep().handleAttemptFailure(event);
    }
    final ClientRetryEvent clientEvent = (ClientRetryEvent) event;
    final TimeDuration timeout = timeoutMap.get(clientEvent.getRequest().getType().getTypeCase());
    if (timeout != null && clientEvent.isRequestTimeout(timeout)) {
      return NO_RETRY_ACTION;
    }
    return Optional.ofNullable(
        retryPolicyMap.get(clientEvent.getRequest().getType().getTypeCase()))
        .orElse(RetryPolicies.retryForeverNoSleep())
        .handleAttemptFailure(event);
  }

  @Override
  public String toString() {
    return myString.get();
  }
}
