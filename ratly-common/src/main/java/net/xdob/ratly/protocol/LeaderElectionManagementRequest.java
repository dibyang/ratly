
package net.xdob.ratly.protocol;

import net.xdob.ratly.util.JavaUtils;

public final class LeaderElectionManagementRequest extends RaftClientRequest{
  public abstract static class Op {

  }
  public static class Pause extends Op {
    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass()) + ":" ;
    }
  }

  public static class Resume extends Op {
    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass()) + ":" ;
    }
  }

  public static LeaderElectionManagementRequest newPause(ClientId clientId,
      RaftPeerId serverId, RaftGroupId groupId, long callId) {
    return new LeaderElectionManagementRequest(clientId,
        serverId, groupId, callId, new Pause());
  }

  public static LeaderElectionManagementRequest newResume(ClientId clientId,
      RaftPeerId serverId, RaftGroupId groupId, long callId) {
    return new LeaderElectionManagementRequest(clientId,
        serverId, groupId, callId, new Resume());
  }

  private final Op op;

  public LeaderElectionManagementRequest(
      ClientId clientId, RaftPeerId serverId, RaftGroupId groupId, long callId, Op op) {
    super(clientId, serverId, groupId, callId, readRequestType());
    this.op = op;
  }

  public Pause getPause() {
    return op instanceof Pause ? (Pause) op: null;
  }

  public Resume getResume() {
    return op instanceof Resume ? (Resume) op : null;
  }


  @Override
  public String toString() {
    return super.toString() + ", " + op;
  }
}
