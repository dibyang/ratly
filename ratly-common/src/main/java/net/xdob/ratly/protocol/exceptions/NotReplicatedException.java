package net.xdob.ratly.protocol.exceptions;

import net.xdob.ratly.proto.RaftProtos.CommitInfoProto;
import net.xdob.ratly.proto.RaftProtos.ReplicationLevel;

import java.util.Collection;

public class NotReplicatedException extends RaftException {
  private final long callId;
  private final ReplicationLevel requiredReplication;
  private final long logIndex;
  /** This is only populated on client-side since RaftClientReply already has commitInfos */
  private Collection<CommitInfoProto> commitInfos;

  public NotReplicatedException(long callId, ReplicationLevel requiredReplication, long logIndex) {
    super("Request with call Id " + callId + " and log index " + logIndex
        + " is not yet replicated to " + requiredReplication);
    this.callId = callId;
    this.requiredReplication = requiredReplication;
    this.logIndex = logIndex;
  }

  public NotReplicatedException(long callId, ReplicationLevel requiredReplication, long logIndex,
                                Collection<CommitInfoProto> commitInfos) {
    this(callId, requiredReplication, logIndex);
    this.commitInfos = commitInfos;
  }

  public long getCallId() {
    return callId;
  }

  public ReplicationLevel getRequiredReplication() {
    return requiredReplication;
  }

  public long getLogIndex() {
    return logIndex;
  }

  public Collection<CommitInfoProto> getCommitInfos() {
    return commitInfos;
  }
}
