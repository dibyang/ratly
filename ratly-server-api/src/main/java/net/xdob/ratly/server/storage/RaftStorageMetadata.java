package net.xdob.ratly.server.storage;

import net.xdob.ratly.proto.raft.TermIndexProto;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.util.JavaUtils;

import java.util.Objects;
import java.util.Optional;

/**
 * 存储 Raft 协议相关的元数据，包括 term（当前任期）和 votedFor（投票的服务器）
 * <p>
 * This is a value-based class.
 */
public final class RaftStorageMetadata {
  private static final RaftStorageMetadata DEFAULT = valueOf(
      TermIndexProto.getDefaultInstance().getTerm(), RaftPeerId.valueOf("",false));

  public static RaftStorageMetadata getDefault() {
    return DEFAULT;
  }

  public static RaftStorageMetadata valueOf(long term, RaftPeerId votedFor) {
    return new RaftStorageMetadata(term, votedFor);
  }

  private final long term;
  private final RaftPeerId votedFor;

  private RaftStorageMetadata(long term, RaftPeerId votedFor) {
    this.term = term;
    this.votedFor = Optional.ofNullable(votedFor).orElseGet(() -> getDefault().getVotedFor());
  }

  /** @return the term. */
  public long getTerm() {
    return term;
  }

  /** @return the server it voted for. */
  public RaftPeerId getVotedFor() {
    return votedFor;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final RaftStorageMetadata that = (RaftStorageMetadata) obj;
    return this.term == that.term && Objects.equals(this.votedFor, that.votedFor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(term, votedFor);
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass()) + "{term=" + term + ", votedFor=" + votedFor + '}';
  }
}
