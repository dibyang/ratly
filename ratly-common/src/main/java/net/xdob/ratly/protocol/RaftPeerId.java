package net.xdob.ratly.protocol;

import net.xdob.ratly.proto.raft.RaftPeerIdProto;
import com.google.protobuf.ByteString;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.Preconditions;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * ID of Raft Peer which is globally unique.
 * <p>
 * This is a value-based class.
 */
public final class RaftPeerId {
  private static final Map<ByteString, RaftPeerId> BYTE_STRING_MAP = new ConcurrentHashMap<>();
  private static final Map<String, RaftPeerId> STRING_MAP = new ConcurrentHashMap<>();

  public static RaftPeerId valueOf(ByteString id) {
    final RaftPeerId cached = BYTE_STRING_MAP.get(id);
    if (cached != null) {
      return cached;
    }
    ByteString cloned = ByteString.copyFrom(id.asReadOnlyByteBuffer());
    return BYTE_STRING_MAP.computeIfAbsent(cloned, RaftPeerId::new);
  }

  public static RaftPeerId valueOf(String id) {
    return STRING_MAP.computeIfAbsent(id, RaftPeerId::new);
  }

  public static RaftPeerId getRaftPeerId(String id) {
    return id == null || id.isEmpty() ? null : RaftPeerId.valueOf(id);
  }

  /** UTF-8 string as id */
  private final String idString;
  /** The corresponding bytes of {@link #idString}. */
  private final ByteString id;

  private final Supplier<RaftPeerIdProto> raftPeerIdProto;

  private RaftPeerId(String id) {
    this.idString = Objects.requireNonNull(id, "id == null");
    this.id = ByteString.copyFrom(idString, StandardCharsets.UTF_8);
    this.raftPeerIdProto = JavaUtils.memoize(this::buildRaftPeerIdProto);
  }

  private RaftPeerId(ByteString id) {
    this.id = Objects.requireNonNull(id, "id == null");
    Preconditions.assertTrue(!id.isEmpty(), "id is empty.");
    this.idString = id.toString(StandardCharsets.UTF_8);
    this.raftPeerIdProto = JavaUtils.memoize(this::buildRaftPeerIdProto);
  }

  private RaftPeerIdProto buildRaftPeerIdProto() {
    return RaftPeerIdProto.newBuilder().setId(id).build();
  }

  public RaftPeerIdProto getRaftPeerIdProto() {
    return raftPeerIdProto.get();
  }

  /**
   * @return id in {@link ByteString}.
   */
  public ByteString toByteString() {
    return id;
  }

  @Override
  public String toString() {
    return idString;
  }

  @Override
  public boolean equals(Object other) {
    return other == this ||
        (other instanceof RaftPeerId && idString.equals(((RaftPeerId)other).idString));
  }

  @Override
  public int hashCode() {
    return idString.hashCode();
  }
}
