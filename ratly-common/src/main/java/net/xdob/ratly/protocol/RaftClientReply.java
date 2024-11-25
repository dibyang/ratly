package net.xdob.ratly.protocol;

import net.xdob.ratly.proto.raft.CommitInfoProto;
import net.xdob.ratly.protocol.exceptions.*;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.ProtoUtils;
import net.xdob.ratly.util.ReflectionUtils;

import java.util.Collection;
import java.util.Collections;

/**
 * Reply from server to client
 */
public class RaftClientReply extends RaftClientMessage {
  /**
   * To build {@link RaftClientReply}
   */
  public static class Builder {
    private ClientId clientId;
    private RaftPeerId serverId;
    private RaftGroupId groupId;
    private long callId;

    private boolean success;
    private Message message;
    private RaftException exception;

    private long logIndex;
    private Collection<CommitInfoProto> commitInfos;

    public RaftClientReply build() {
      return new RaftClientReply(clientId, serverId, groupId, callId,
          success, message, exception, logIndex, commitInfos);
    }

    public Builder setClientId(ClientId clientId) {
      this.clientId = clientId;
      return this;
    }

    public Builder setServerId(RaftPeerId serverId) {
      this.serverId = serverId;
      return this;
    }

    public Builder setGroupId(RaftGroupId groupId) {
      this.groupId = groupId;
      return this;
    }

    public Builder setCallId(long callId) {
      this.callId = callId;
      return this;
    }

    public Builder setSuccess(boolean success) {
      this.success = success;
      return this;
    }

    public Builder setSuccess() {
      return setSuccess(true);
    }

    public Builder setException(RaftException exception) {
      this.exception = exception;
      return this;
    }

    public Builder setMessage(Message message) {
      this.message = message;
      return this;
    }

    public Builder setLogIndex(long logIndex) {
      this.logIndex = logIndex;
      return this;
    }

    public Builder setCommitInfos(Collection<CommitInfoProto> commitInfos) {
      this.commitInfos = commitInfos;
      return this;
    }

    public Builder setServerId(RaftGroupMemberId serverId) {
      return setServerId(serverId.getPeerId())
          .setGroupId(serverId.getGroupId());
    }

    public Builder setClientInvocationId(ClientInvocationId invocationId) {
      return setClientId(invocationId.getClientId())
          .setCallId(invocationId.getLongId());
    }

    public Builder setRequest(RaftClientRequest request) {
      return setClientId(request.getClientId())
          .setServerId(request.getServerId())
          .setGroupId(request.getRaftGroupId())
          .setCallId(request.getCallId());
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final boolean success;

  /**
   * We mainly track two types of exceptions here:
   * 1. NotLeaderException if the server is not leader
   * 2. StateMachineException if the server's state machine returns an exception
   */
  private final RaftException exception;
  private final Message message;

  /**
   * This field is the log index of the transaction
   * if (1) the request is {@link net.xdob.ratly.proto.raft.RaftClientRequestProto.TypeCase#WRITE} and (2) the
   * reply is success.
   * Otherwise, this field is not used.
   */
  private final long logIndex;
  /** The commit information when the reply is created. */
  private final Collection<CommitInfoProto> commitInfos;

  @SuppressWarnings("parameternumber")
  RaftClientReply(ClientId clientId, RaftPeerId serverId, RaftGroupId groupId,
      long callId, boolean success, Message message, RaftException exception,
      long logIndex, Collection<CommitInfoProto> commitInfos) {
    super(clientId, serverId, groupId, callId);
    this.success = success;
    this.message = message;
    this.exception = exception;
    this.logIndex = logIndex;
    this.commitInfos = commitInfos != null? commitInfos: Collections.emptyList();

    if (exception != null) {
      Preconditions.assertTrue(!success,
          () -> "Inconsistent parameters: success && exception != null: " + this);
      Preconditions.assertTrue(ReflectionUtils.isInstance(exception,
          AlreadyClosedException.class,
          NotLeaderException.class, NotReplicatedException.class,
          LeaderNotReadyException.class, StateMachineException.class, DataStreamException.class,
          LeaderSteppingDownException.class, TransferLeadershipException.class, ReadException.class,
          ReadIndexException.class),
          () -> "Unexpected exception class: " + this);
    }
  }

  /**
   * Get the commit information for the entire group.
   * The commit information may be unavailable for exception reply.
   *
   * @return the commit information if it is available; otherwise, return null.
   */
  public Collection<CommitInfoProto> getCommitInfos() {
    return commitInfos;
  }

  @Override
  public final boolean isRequest() {
    return false;
  }

  public long getLogIndex() {
    return logIndex;
  }

  @Override
  public String toString() {
    return super.toString() + ", "
        + (isSuccess()? "SUCCESS":  "FAILED " + exception)
        + ", logIndex=" + getLogIndex() + ", commits" + ProtoUtils.toString(commitInfos);
  }

  public boolean isSuccess() {
    return success;
  }

  public Message getMessage() {
    return message;
  }

  /** If this reply has {@link AlreadyClosedException}, return it; otherwise return null. */
  public AlreadyClosedException getAlreadyClosedException() {
    return JavaUtils.cast(exception, AlreadyClosedException.class);
  }

  /** If this reply has {@link NotLeaderException}, return it; otherwise return null. */
  public NotLeaderException getNotLeaderException() {
    return JavaUtils.cast(exception, NotLeaderException.class);
  }

  /** If this reply has {@link NotReplicatedException}, return it; otherwise return null. */
  public NotReplicatedException getNotReplicatedException() {
    return JavaUtils.cast(exception, NotReplicatedException.class);
  }

  /** If this reply has {@link StateMachineException}, return it; otherwise return null. */
  public StateMachineException getStateMachineException() {
    return JavaUtils.cast(exception, StateMachineException.class);
  }

  /** If this reply has {@link DataStreamException}, return it; otherwise return null. */
  public DataStreamException getDataStreamException() {
    return JavaUtils.cast(exception, DataStreamException.class);
  }

  public LeaderNotReadyException getLeaderNotReadyException() {
    return JavaUtils.cast(exception, LeaderNotReadyException.class);
  }

  public LeaderSteppingDownException getLeaderSteppingDownException() {
    return JavaUtils.cast(exception, LeaderSteppingDownException.class);
  }

  public TransferLeadershipException getTransferLeadershipException() {
    return JavaUtils.cast(exception, TransferLeadershipException.class);
  }

  public ReadException getReadException() {
    return JavaUtils.cast(exception, ReadException.class);
  }

  public ReadIndexException getReadIndexException() {
    return JavaUtils.cast(exception, ReadIndexException.class);
  }

  /** @return the exception, if there is any; otherwise, return null. */
  public RaftException getException() {
    return exception;
  }
}
