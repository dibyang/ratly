
package net.xdob.ratly.server.impl;

import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.proto.RaftProtos.CommitInfoProto;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.ProtoUtils;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Caching the commit information. */
class CommitInfoCache {
  private final ConcurrentMap<RaftPeerId, Long> map = new ConcurrentHashMap<>();

  Optional<Long> get(RaftPeerId id) {
    return Optional.ofNullable(map.get(id));
  }

  CommitInfoProto update(RaftPeer peer, long newCommitIndex) {
    Objects.requireNonNull(peer, "peer == null");
    final long updated = update(peer.getId(), newCommitIndex);
    return ProtoUtils.toCommitInfoProto(peer, updated);
  }

  long update(RaftPeerId peerId, long newCommitIndex) {
    Objects.requireNonNull(peerId, "peerId == null");
    return map.compute(peerId, (id, oldCommitIndex) -> {
      if (oldCommitIndex != null) {
        // get around BX_UNBOXING_IMMEDIATELY_REBOXED
        final long old = oldCommitIndex;
        if (old >= newCommitIndex) {
          return old;
        }
      }
      return newCommitIndex;
    });
  }

  void update(CommitInfoProto newInfo) {
    final RaftPeerId id = RaftPeerId.valueOf(newInfo.getServer().getId());
    update(id, newInfo.getCommitIndex());
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass()) + ":" + map;
  }
}
