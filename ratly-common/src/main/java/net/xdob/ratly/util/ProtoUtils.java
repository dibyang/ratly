package net.xdob.ratly.util;

import net.xdob.ratly.proto.RaftProtos;
import net.xdob.ratly.proto.RaftProtos.*;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftGroupMemberId;
import net.xdob.ratly.protocol.*;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.stream.Collectors;

public interface ProtoUtils {
  static ByteString writeObject2ByteString(Object obj) {
    final ByteString.Output byteOut = ByteString.newOutput();
    try(ObjectOutputStream objOut = new ObjectOutputStream(byteOut)) {
      objOut.writeObject(obj);
    } catch (IOException e) {
      throw new IllegalStateException(
          "Unexpected IOException when writing an object to a ByteString.", e);
    }
    return byteOut.toByteString();
  }

  static Object toObject(ByteString bytes) {
    return IOUtils.readObject(bytes.newInput(), Object.class);
  }

  static ThrowableProto toThrowableProto(Throwable t) {
    final ThrowableProto.Builder builder = ThrowableProto.newBuilder()
        .setClassName(t.getClass().getName())
        .setErrorMessage(t.getMessage())
        .setStackTrace(writeObject2ByteString(t.getStackTrace()));
    Optional.ofNullable(t.getCause())
        .map(ProtoUtils::writeObject2ByteString)
        .ifPresent(builder::setCause);
    return builder.build();
  }

  static <T extends Throwable> T toThrowable(ThrowableProto proto, Class<T> clazz) {
    Preconditions.assertTrue(clazz.getName().equals(proto.getClassName()),
        () -> "Unexpected class " + proto.getClassName() + ", expecting " + clazz + ", proto=" + proto);

    final T throwable ;
    try {
      throwable = ReflectionUtils.instantiateException(clazz, proto.getErrorMessage());
    } catch(Exception e) {
      throw new IllegalStateException("Failed to create a new object from " + clazz + ", proto=" + proto, e);
    }

    Optional.ofNullable(proto.getStackTrace())
        .filter(b -> !b.isEmpty())
        .map(ProtoUtils::toObject)
        .map(obj -> JavaUtils.cast(obj, StackTraceElement[].class))
        .ifPresent(throwable::setStackTrace);
    Optional.ofNullable(proto.getCause())
        .filter(b -> !b.isEmpty())
        .map(ProtoUtils::toObject)
        .map(obj -> JavaUtils.cast(obj, Throwable.class))
        .ifPresent(throwable::initCause);
    return throwable;
  }

  static ByteString toByteString(String string) {
    return ByteString.copyFromUtf8(string);
  }

  static ByteString toByteString(byte[] bytes) {
    return toByteString(bytes, 0, bytes.length);
  }

  static ByteString toByteString(byte[] bytes, int offset, int size) {
    // return singleton to reduce object allocation
    return bytes.length == 0 ?
        ByteString.EMPTY : ByteString.copyFrom(bytes, offset, size);
  }

  static RaftPeer toRaftPeer(RaftPeerProto p) {
    return RaftPeer.newBuilder()
        .setId(RaftPeerId.valueOf(p.getId()))
        .setAddress(p.getAddress())
        .setDataStreamAddress(p.getDataStreamAddress())
        .setClientAddress(p.getClientAddress())
        .setAdminAddress(p.getAdminAddress())
        .setPriority(p.getPriority())
        .setStartupRole(p.hasStartupRole() ? p.getStartupRole() : RaftProtos.RaftPeerRole.FOLLOWER)
        .build();
  }

  static List<RaftPeer> toRaftPeers(List<RaftPeerProto> protos) {
    return protos.stream().map(ProtoUtils::toRaftPeer).collect(Collectors.toList());
  }

  static Iterable<RaftPeerProto> toRaftPeerProtos(
      final Collection<RaftPeer> peers) {
    return () -> new Iterator<RaftPeerProto>() {
      private final Iterator<RaftPeer> i = peers.iterator();

      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public RaftPeerProto next() {
        return i.next().getRaftPeerProto();
      }
    };
  }

  static Iterable<RaftPeerIdProto> toRaftPeerIdProtos(
      final Collection<RaftPeerId> peers) {
    return () -> new Iterator<RaftPeerIdProto>() {
      private final Iterator<RaftPeerId> i = peers.iterator();

      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public RaftPeerIdProto next() {
        return i.next().getRaftPeerIdProto();
      }
    };
  }

  static Iterable<RouteProto> toRouteProtos(
      final Map<RaftPeerId, Set<RaftPeerId>> routingTable) {
    return () -> new Iterator<RouteProto>() {
      private final Iterator<Map.Entry<RaftPeerId, Set<RaftPeerId>>> i = routingTable.entrySet().iterator();

      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public RouteProto next() {
        Map.Entry<RaftPeerId, Set<RaftPeerId>> entry = i.next();
        return RouteProto.newBuilder()
            .setPeerId(entry.getKey().getRaftPeerIdProto())
            .addAllSuccessors(toRaftPeerIdProtos(entry.getValue()))
            .build();
      }
    };
  }

  static RaftGroupId toRaftGroupId(RaftGroupIdProto proto) {
    return RaftGroupId.valueOf(proto.getId());
  }

  static RaftGroupIdProto.Builder toRaftGroupIdProtoBuilder(RaftGroupId id) {
    return RaftGroupIdProto.newBuilder().setId(id.toByteString());
  }

  static RaftGroup toRaftGroup(RaftGroupProto proto) {
    return RaftGroup.valueOf(toRaftGroupId(proto.getGroupId()), toRaftPeers(proto.getPeersList()));
  }

  static RaftGroupProto.Builder toRaftGroupProtoBuilder(RaftGroup group) {
    return RaftGroupProto.newBuilder()
        .setGroupId(toRaftGroupIdProtoBuilder(group.getGroupId()))
        .addAllPeers(toRaftPeerProtos(group.getPeers()));
  }

  static RaftGroupMemberId toRaftGroupMemberId(ByteString peerId, RaftGroupIdProto groupId) {
    return RaftGroupMemberId.valueOf(RaftPeerId.valueOf(peerId), ProtoUtils.toRaftGroupId(groupId));
  }

  static RaftGroupMemberId toRaftGroupMemberId(RaftGroupMemberIdProto memberId) {
    return toRaftGroupMemberId(memberId.getPeerId(), memberId.getGroupId());
  }

  static RaftGroupMemberIdProto.Builder toRaftGroupMemberIdProtoBuilder(RaftGroupMemberId memberId) {
    return RaftGroupMemberIdProto.newBuilder()
        .setPeerId(memberId.getPeerId().toByteString())
        .setGroupId(toRaftGroupIdProtoBuilder(memberId.getGroupId()));
  }

  static CommitInfoProto toCommitInfoProto(RaftPeer peer, long commitIndex) {
    return CommitInfoProto.newBuilder()
        .setServer(peer.getRaftPeerProto())
        .setCommitIndex(commitIndex)
        .build();
  }

  static String toString(CommitInfoProto proto) {
    return RaftPeerId.valueOf(proto.getServer().getId()) + ":c" + proto.getCommitIndex();
  }

  static String toString(Collection<CommitInfoProto> protos) {
    return protos.stream().map(ProtoUtils::toString).collect(Collectors.toList()).toString();
  }

  static SlidingWindowEntry toSlidingWindowEntry(long seqNum, boolean isFirst) {
    return SlidingWindowEntry.newBuilder().setSeqNum(seqNum).setIsFirst(isFirst).build();
  }

  static String toString(SlidingWindowEntry proto) {
    if (proto == null) {
      return null;
    }
    return proto.getSeqNum() + (proto.getIsFirst()? "*": "");
  }

  static String toString(RaftRpcRequestProto proto) {
    return proto.getRequestorId().toStringUtf8() + "->" + proto.getReplyId().toStringUtf8()
        + "#" + proto.getCallId();
  }

  static String toString(RaftRpcReplyProto proto) {
    return proto.getRequestorId().toStringUtf8() + "<-" + proto.getReplyId().toStringUtf8()
        + "#" + proto.getCallId() + ":"
        + (proto.getSuccess()? "OK": "FAIL");
  }
}
