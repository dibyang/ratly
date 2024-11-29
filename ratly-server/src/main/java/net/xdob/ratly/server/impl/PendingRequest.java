package net.xdob.ratly.server.impl;

import net.xdob.ratly.proto.raft.RaftClientRequestProto.TypeCase;
import net.xdob.ratly.proto.raft.CommitInfoProto;
import net.xdob.ratly.protocol.*;
import net.xdob.ratly.protocol.exceptions.NotLeaderException;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.statemachine.TransactionContext;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.Preconditions;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

class PendingRequest {
  private final TermIndex termIndex;
  private final RaftClientRequest request;
  private final TransactionContext entry;
  private final CompletableFuture<RaftClientReply> futureToComplete = new CompletableFuture<>();
  private final CompletableFuture<RaftClientReply> futureToReturn;

  PendingRequest(RaftClientRequest request, TransactionContext entry) {
    this.termIndex = entry == null? null: TermIndex.valueOf(entry.getLogEntryUnsafe());
    this.request = request;
    this.entry = entry;
    if (request.is(TypeCase.FORWARD)) {
      futureToReturn = futureToComplete.thenApply(reply -> convert(request, reply));
    } else {
      futureToReturn = futureToComplete;
    }
  }

  PendingRequest(SetConfigurationRequest request) {
    this(request, null);
  }

  RaftClientReply convert(RaftClientRequest q, RaftClientReply p) {
    return RaftClientReply.newBuilder()
        .setRequest(q)
        .setCommitInfos(p.getCommitInfos())
        .setLogIndex(p.getLogIndex())
        .setMessage(p.getMessage())
        .setException(p.getException())
        .setSuccess(p.isSuccess())
        .build();
  }

  TermIndex getTermIndex() {
    return Objects.requireNonNull(termIndex, "termIndex == null");
  }

  RaftClientRequest getRequest() {
    return request;
  }

  public CompletableFuture<RaftClientReply> getFuture() {
    return futureToReturn;
  }

  TransactionContext getEntry() {
    return entry;
  }

  /**
   * This is only used when setting new raft configuration.
   */
  synchronized void setException(Throwable e) {
    Preconditions.assertTrue(e != null);
    futureToComplete.completeExceptionally(e);
  }

  synchronized void setReply(RaftClientReply r) {
    Preconditions.assertTrue(r != null);
    futureToComplete.complete(r);
  }

  TransactionContext setNotLeaderException(NotLeaderException nle, Collection<CommitInfoProto> commitInfos) {
    setReply(RaftClientReply.newBuilder()
        .setRequest(getRequest())
        .setException(nle)
        .setCommitInfos(commitInfos)
        .build());
    return getEntry();
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass()) + "-" + termIndex + ":request=" + request;
  }
}
