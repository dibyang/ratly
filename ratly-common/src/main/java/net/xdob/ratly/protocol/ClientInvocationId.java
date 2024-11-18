
package net.xdob.ratly.protocol;

import net.xdob.ratly.proto.RaftProtos.StateMachineLogEntryProto;

import java.util.Objects;
import java.util.Optional;

/**
 * The id of a client invocation.
 * A client invocation may be an RPC or a stream.
 *
 * This is a value-based class.
 */
public final class ClientInvocationId {
  public static ClientInvocationId valueOf(ClientId clientId, long invocationId) {
    return new ClientInvocationId(clientId, invocationId);
  }

  public static ClientInvocationId valueOf(RaftClientMessage message) {
    return valueOf(message.getClientId(), message.getCallId());
  }

  public static ClientInvocationId valueOf(StateMachineLogEntryProto proto) {
    return valueOf(ClientId.valueOf(proto.getClientId()), proto.getCallId());
  }

  private final ClientId clientId;
  /** It may be a call id or a stream id. */
  private final long longId;

  private ClientInvocationId(ClientId clientId, long longId) {
    this.clientId = clientId;
    this.longId = longId;
  }

  public ClientId getClientId() {
    return clientId;
  }

  public long getLongId() {
    return longId;
  }

  public boolean match(StateMachineLogEntryProto proto) {
    return longId == proto.getCallId() && Optional.ofNullable(clientId)
        .map(RaftId::toByteString)
        .filter(b -> b.equals(proto.getClientId()))
        .isPresent();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final ClientInvocationId that = (ClientInvocationId) obj;
    return this.longId == that.longId && Objects.equals(this.clientId, that.clientId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clientId, longId);
  }

  @Override
  public String toString() {
    return longId + "@" + clientId;
  }
}
