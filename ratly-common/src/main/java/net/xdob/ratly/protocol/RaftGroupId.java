package net.xdob.ratly.protocol;

import com.google.protobuf.ByteString;

import java.util.UUID;

/**
 * The id of a raft group.
 * <p>
 * This is a value-based class.
 */
public final class RaftGroupId extends RaftId {
  private static final Factory<RaftGroupId> FACTORY = new Factory<RaftGroupId>() {
    @Override
    RaftGroupId newInstance(UUID uuid) {
      return new RaftGroupId(uuid);
    }
  };

  public static RaftGroupId emptyGroupId() {
    return FACTORY.emptyId();
  }

  public static RaftGroupId randomId() {
    return FACTORY.randomId();
  }

  public static RaftGroupId valueOf(UUID uuid) {
    return FACTORY.valueOf(uuid);
  }

  public static RaftGroupId valueOf(ByteString bytes) {
    return FACTORY.valueOf(bytes);
  }

  private RaftGroupId(UUID id) {
    super(id);
  }

  @Override
  String createUuidString(UUID uuid) {
    return "group-" + super.createUuidString(uuid);
  }
}
