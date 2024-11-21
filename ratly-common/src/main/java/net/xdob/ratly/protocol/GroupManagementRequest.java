package net.xdob.ratly.protocol;

import net.xdob.ratly.util.JavaUtils;

public final class GroupManagementRequest extends RaftClientRequest {
  public abstract static class Op {
    public abstract RaftGroupId getGroupId();
  }

  public static class Add extends Op {
    private final RaftGroup group;
    private final boolean format;

    public Add(RaftGroup group, boolean format) {
      this.group = group;
      this.format = format;
    }

    @Override
    public RaftGroupId getGroupId() {
      return getGroup().getGroupId();
    }

    public RaftGroup getGroup() {
      return group;
    }

    public boolean isFormat() {
      return format;
    }

    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass()) + ":" + getGroup();
    }
  }

  public static class Remove extends Op {
    private final RaftGroupId groupId;
    private final boolean deleteDirectory;
    private final boolean renameDirectory;

    public Remove(RaftGroupId groupId, boolean deleteDirectory,
        boolean renameDirectory) {
      this.groupId = groupId;
      this.deleteDirectory = deleteDirectory;
      this.renameDirectory = renameDirectory;
    }

    @Override
    public RaftGroupId getGroupId() {
      return groupId;
    }

    public boolean isDeleteDirectory() {
      return deleteDirectory;
    }

    public boolean isRenameDirectory() {
      return renameDirectory;
    }

    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass()) + ":" + getGroupId() + ", "
          + (deleteDirectory? "delete": (renameDirectory ? "rename" : "retain"))
          + "-dir";
    }
  }

  /** The same as newAdd(clientId, serverId, callId, group, true). */
  public static GroupManagementRequest newAdd(ClientId clientId, RaftPeerId serverId, long callId, RaftGroup group) {
    return newAdd(clientId, serverId, callId, group, true);
  }

  public static GroupManagementRequest newAdd(ClientId clientId, RaftPeerId serverId, long callId,
      RaftGroup group, boolean format) {
    return new GroupManagementRequest(clientId, serverId, callId, new Add(group, format));
  }

  public static GroupManagementRequest newRemove(ClientId clientId, RaftPeerId serverId, long callId,
      RaftGroupId groupId, boolean deleteDirectory, boolean renameDirectory) {
    return new GroupManagementRequest(clientId, serverId, callId,
        new Remove(groupId, deleteDirectory, renameDirectory));
  }

  private final Op op;

  private GroupManagementRequest(ClientId clientId, RaftPeerId serverId, long callId, Op op) {
    super(clientId, serverId, op.getGroupId(), callId, writeRequestType());
    this.op = op;
  }

  public Add getAdd() {
    return op instanceof Add? (Add)op: null;
  }

  public Remove getRemove() {
    return op instanceof Remove? (Remove)op: null;
  }

  @Override
  public String toString() {
    return super.toString() + ", " + op;
  }
}
