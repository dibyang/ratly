package net.xdob.ratly.protocol;

import net.xdob.ratly.proto.raft.RaftPeerIdProto;
import com.google.protobuf.ByteString;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.Preconditions;

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
  /**
   * 虚拟节点物理ID前缀
   */
  private static final String VIRTUAL_PREFIX = "vn_";

  public static RaftPeerId valueOf(ByteString id) {
    final RaftPeerId cached = BYTE_STRING_MAP.get(id);
    if (cached != null) {
      return cached;
    }
    ByteString cloned = ByteString.copyFrom(id.asReadOnlyByteBuffer());
    return BYTE_STRING_MAP.computeIfAbsent(cloned, RaftPeerId::new);
  }

  public static RaftPeerId valueOf(String uid) {
    return STRING_MAP.computeIfAbsent(uid, RaftPeerId::new);
  }

  public static RaftPeerId valueOf(String id, boolean virtual) {
    return valueOf(RaftPeerId.buildUid(id, virtual));
  }


  public static RaftPeerId getRaftPeerId(String uid) {
    return uid == null || uid.isEmpty() ? null : RaftPeerId.valueOf(uid);
  }

  /** UTF-8 string as uid */
  private final String uid;
  /** The corresponding bytes of {@link #uid}. */
  private final ByteString idBytes;

  private final Supplier<RaftPeerIdProto> raftPeerIdProto;

  private RaftPeerId(String uid) {
    this.uid = Objects.requireNonNull(uid, "uid == null");
    this.idBytes = ByteString.copyFromUtf8(uid);
    this.raftPeerIdProto = JavaUtils.memoize(this::buildRaftPeerIdProto);
  }

  private RaftPeerId(String id, boolean virtual) {
    this(buildUid(id, virtual));
  }

  public static String buildUid(String id, boolean virtual) {
    return virtual? VIRTUAL_PREFIX +id:id;
  }

  private RaftPeerId(ByteString idBytes) {
    this.idBytes = Objects.requireNonNull(idBytes, "id == null");
    Preconditions.assertTrue(!idBytes.isEmpty(), "id is empty.");
    this.uid = idBytes.toStringUtf8();
    this.raftPeerIdProto = JavaUtils.memoize(this::buildRaftPeerIdProto);
  }

  private RaftPeerIdProto buildRaftPeerIdProto() {
    return RaftPeerIdProto.newBuilder().setId(idBytes).build();
  }

  public RaftPeerIdProto getRaftPeerIdProto() {
    return raftPeerIdProto.get();
  }

  /**
   * @return id in {@link ByteString}.
   */
  public ByteString toByteString() {
    return idBytes;
  }

  public String getOwnerId() {
    return isVirtual()?uid.substring(VIRTUAL_PREFIX.length()):"";
  }

  public String getId() {
    return uid;
  }

  /**
   * 是否虚拟节点
   */
  public boolean isVirtual() {
    return uid.startsWith(VIRTUAL_PREFIX);
  }

  @Override
  public String toString() {
    return uid;
  }

  @Override
  public boolean equals(Object other) {
    return other == this ||
        (other instanceof RaftPeerId && uid.equals(((RaftPeerId)other).uid));
  }

  @Override
  public int hashCode() {
    return uid.hashCode();
  }

  public String getHostId(){
    return isVirtual() ? getOwnerId() : getId();
  }

  public boolean isOwner(RaftPeerId peerId){
    return getHostId().equals(peerId.getHostId());
  }
}
