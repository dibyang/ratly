package net.xdob.ratly.protocol;

import com.google.protobuf.ByteString;

import java.util.UUID;

/**
 * The id of RaftClient. Should be globally unique so that raft peers can use it
 * to correctly identify retry requests from the same client.
 */
public final class ClientId extends RaftId {
  private static final Factory<ClientId> FACTORY = new Factory<ClientId>() {
    @Override
    ClientId newInstance(UUID uuid) {
      return new ClientId(uuid);
    }
  };

  public static ClientId emptyClientId() {
    return FACTORY.emptyId();
  }

  public static ClientId randomId() {
    return FACTORY.randomId();
  }

  public static ClientId valueOf(ByteString bytes) {
    return FACTORY.valueOf(bytes);
  }

  public static ClientId valueOf(UUID uuid) {
    return FACTORY.valueOf(uuid);
  }

  private ClientId(UUID uuid) {
    super(uuid);
  }

  @Override
  String createUuidString(UUID uuid) {
    return "client-" + super.createUuidString(uuid);
  }
}
