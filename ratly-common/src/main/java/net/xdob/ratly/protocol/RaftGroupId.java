package net.xdob.ratly.protocol;

import com.google.protobuf.ByteString;

/**
 * The id of a raft group.
 * <p>
 * This is a value-based class.
 */
public final class RaftGroupId extends RaftId {
  private static final Factory<RaftGroupId> FACTORY = new Factory<RaftGroupId>() {
    @Override
    RaftGroupId newInstance(String id) {
      return new RaftGroupId(id);
    }
  };

  public static RaftGroupId emptyGroupId() {
    return FACTORY.emptyId();
  }

  public static RaftGroupId randomId() {
    return FACTORY.randomId();
  }

  public static RaftGroupId valueOf(String id) {
    return FACTORY.valueOf(id);
  }



  public static RaftGroupId valueOf(ByteString bytes) {
    return FACTORY.valueOf(bytes);
  }

  private RaftGroupId(String id) {
    super(id);
  }

}
