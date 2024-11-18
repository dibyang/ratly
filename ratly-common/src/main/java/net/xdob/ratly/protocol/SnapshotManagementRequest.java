
package net.xdob.ratly.protocol;

import net.xdob.ratly.util.JavaUtils;

public final class SnapshotManagementRequest extends RaftClientRequest {

  public abstract static class Op {

  }

  public static final class Create extends Op {
    private final long creationGap;
    private Create(long creationGap) {
      this.creationGap = creationGap;
    }

    public long getCreationGap() {
      return creationGap;
    }

    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass()) + ":" ;
    }

  }

  public static SnapshotManagementRequest newCreate(ClientId clientId,
      RaftPeerId serverId, RaftGroupId groupId, long callId, long timeoutMs) {
    return newCreate(clientId, serverId, groupId, callId, timeoutMs, 0);
  }

  public static SnapshotManagementRequest newCreate(ClientId clientId,
      RaftPeerId serverId, RaftGroupId groupId, long callId, long timeoutMs, long creationGap) {
    return new SnapshotManagementRequest(clientId,
        serverId, groupId, callId, timeoutMs, new Create(creationGap));
  }

  private final Op op;

  public SnapshotManagementRequest(ClientId clientId,
      RaftPeerId serverId, RaftGroupId groupId, long callId, long timeoutMs, Op op) {
    super(clientId, serverId, groupId, callId, readRequestType(), timeoutMs);
    this.op = op;
  }

  public Create getCreate() {
    return op instanceof Create ? (Create)op: null;
  }

  @Override
  public String toString() {
    return super.toString() + ", " + op;
  }
}
