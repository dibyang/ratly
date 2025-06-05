package net.xdob.ratly.server.impl;

import net.xdob.ratly.client.impl.ClientProtoUtils;
import net.xdob.ratly.proto.raft.*;
import net.xdob.ratly.proto.raft.AppendEntriesReplyProto.AppendResult;
import net.xdob.ratly.protocol.RaftClientRequest;
import net.xdob.ratly.protocol.RaftGroupMemberId;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.util.Preconditions;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/** Server proto utilities for internal use. */
final class ServerProtoUtils {
  private ServerProtoUtils() {}

  private static RaftRpcReplyProto.Builder toRaftRpcReplyProtoBuilder(
      RaftPeerId requestorId, RaftGroupMemberId replyId, boolean success) {
    return ClientProtoUtils.toRaftRpcReplyProtoBuilder(
        requestorId.toByteString(), replyId.getPeerId().toByteString(), replyId.getGroupId(), null, success);
  }

  static RequestVoteReplyProto toRequestVoteReplyProto(
      RaftPeerId requestorId, RaftGroupMemberId replyId, boolean success, long term, boolean shouldShutdown) {
    return RequestVoteReplyProto.newBuilder()
        .setServerReply(toRaftRpcReplyProtoBuilder(requestorId, replyId, success))
        .setTerm(term)
        .setShouldShutdown(shouldShutdown)
        .build();
  }

  static RequestVoteRequestProto toRequestVoteRequestProto(
      RaftGroupMemberId requestorId, RaftPeerId replyId, long term, TermIndex lastEntry, boolean preVote) {
    final RequestVoteRequestProto.Builder b = RequestVoteRequestProto.newBuilder()
        .setServerRequest(ClientProtoUtils.toRaftRpcRequestProtoBuilder(requestorId, replyId))
        .setCandidateTerm(term)
        .setPreVote(preVote);
    Optional.ofNullable(lastEntry).map(TermIndex::toProto).ifPresent(b::setCandidateLastEntry);
    return b.build();
  }

  static StartLeaderElectionReplyProto toStartLeaderElectionReplyProto(
      RaftPeerId requestorId, RaftGroupMemberId replyId, boolean success) {
    return StartLeaderElectionReplyProto.newBuilder()
        .setServerReply(toRaftRpcReplyProtoBuilder(requestorId, replyId, success))
        .build();
  }

  static StartLeaderElectionRequestProto toStartLeaderElectionRequestProto(
      RaftGroupMemberId requestorId, RaftPeerId replyId, TermIndex lastEntry) {
    final StartLeaderElectionRequestProto.Builder b = StartLeaderElectionRequestProto.newBuilder()
        .setServerRequest(ClientProtoUtils.toRaftRpcRequestProtoBuilder(requestorId, replyId));
    if (lastEntry != null) {
      b.setLeaderLastEntry(lastEntry.toProto());
    }
    return b.build();
  }

  static InstallSnapshotReplyProto toInstallSnapshotReplyProto(
      RaftPeerId requestorId, RaftGroupMemberId replyId,
      long currentTerm, int requestIndex, InstallSnapshotResult result) {
    final RaftRpcReplyProto.Builder rb = toRaftRpcReplyProtoBuilder(requestorId,
        replyId, isSuccess(result));
    final InstallSnapshotReplyProto.Builder builder = InstallSnapshotReplyProto
        .newBuilder().setServerReply(rb).setTerm(currentTerm).setResult(result)
        .setRequestIndex(requestIndex);
    return builder.build();
  }

  static InstallSnapshotReplyProto toInstallSnapshotReplyProto(
      RaftPeerId requestorId, RaftGroupMemberId replyId,
      long currentTerm, InstallSnapshotResult result, long installedSnapshotIndex) {
    final boolean success = isSuccess(result);
    Preconditions.assertTrue(success || installedSnapshotIndex == RaftLog.INVALID_LOG_INDEX,
        () -> "result=" + result + " but installedSnapshotIndex=" + installedSnapshotIndex);
    final RaftRpcReplyProto.Builder rb = toRaftRpcReplyProtoBuilder(requestorId, replyId, success);
    return InstallSnapshotReplyProto.newBuilder()
        .setServerReply(rb)
        .setTerm(currentTerm)
        .setResult(result)
        .setSnapshotIndex(installedSnapshotIndex > 0? installedSnapshotIndex: 0)
        .build();
  }

  static InstallSnapshotReplyProto toInstallSnapshotReplyProto(
      RaftPeerId requestorId, RaftGroupMemberId replyId,
      long currentTerm, InstallSnapshotResult result) {
    return toInstallSnapshotReplyProto(requestorId, replyId, currentTerm, result, RaftLog.INVALID_LOG_INDEX);
  }

  static ReadIndexRequestProto toReadIndexRequestProto(
      RaftClientRequest clientRequest, RaftGroupMemberId requestorId, RaftPeerId replyId) {
    return ReadIndexRequestProto.newBuilder()
        .setServerRequest(ClientProtoUtils.toRaftRpcRequestProtoBuilder(requestorId, replyId))
        .setClientRequest(ClientProtoUtils.toRaftClientRequestProto(clientRequest, false))
        .build();
  }

  static ReadIndexReplyProto toReadIndexReplyProto(
      RaftPeerId requestorId, RaftGroupMemberId replyId, boolean success, long index) {
    return ReadIndexReplyProto.newBuilder()
        .setServerReply(toRaftRpcReplyProtoBuilder(requestorId, replyId, success))
        .setReadIndex(index)
        .build();
  }

  static ReadIndexReplyProto toReadIndexReplyProto(RaftPeerId requestorId, RaftGroupMemberId replyId) {
    return toReadIndexReplyProto(requestorId, replyId, false, RaftLog.INVALID_LOG_INDEX);
  }

  @SuppressWarnings("parameternumber")
  static AppendEntriesReplyProto toAppendEntriesReplyProto(
      RaftPeerId requestorId, RaftGroupMemberId replyId, long term,
      long followerCommit, long nextIndex, AppendResult result, long callId,
      long matchIndex, boolean isHeartbeat, boolean started) {
    RaftRpcReplyProto.Builder rpcReply = toRaftRpcReplyProtoBuilder(
        requestorId, replyId, result == AppendResult.SUCCESS)
        .setCallId(callId);
    return AppendEntriesReplyProto.newBuilder()
        .setServerReply(rpcReply)
        .setTerm(term)
        .setNextIndex(nextIndex)
        .setMatchIndex(matchIndex)
        .setFollowerCommit(followerCommit)
        .setResult(result)
        .setIsHearbeat(isHeartbeat)
        .setStarted(started)
        .build();
  }

  @SuppressWarnings("checkstyle:parameternumber")
  static AppendEntriesRequestProto toAppendEntriesRequestProto(
      RaftGroupMemberId requestorId, RaftPeerId replyId, long leaderTerm,
      List<LogEntryProto> entries, long leaderCommit, boolean initializing,
      String vnPeerId,
      TermIndex previous, Collection<CommitInfoProto> commitInfos, long callId) {
    final RaftRpcRequestProto.Builder rpcRequest = ClientProtoUtils.toRaftRpcRequestProtoBuilder(requestorId, replyId)
        .setCallId(callId);
    final AppendEntriesRequestProto.Builder b = AppendEntriesRequestProto
        .newBuilder()
        .setServerRequest(rpcRequest)
        .setLeaderTerm(leaderTerm)
        .setLeaderCommit(leaderCommit)
        .setInitializing(initializing)
        .setVnPeerId(vnPeerId);
    if (entries != null && !entries.isEmpty()) {
      b.addAllEntries(entries);
    }

    Optional.ofNullable(previous).map(TermIndex::toProto).ifPresent(b::setPreviousLog);
    Optional.ofNullable(commitInfos).ifPresent(b::addAllCommitInfos);
    return b.build();
  }

  static ServerRpcProto toServerRpcProto(RaftPeer peer, long delay) {
    if (peer == null) {
      // if no peer information return empty
      return ServerRpcProto.getDefaultInstance();
    }
    return ServerRpcProto.newBuilder()
        .setId(peer.getRaftPeerProto())
        .setLastRpcElapsedTimeMs(delay)
        .build();
  }

  static boolean isSuccess(InstallSnapshotResult result) {
    switch (result) {
      case SUCCESS:
      case SNAPSHOT_INSTALLED:
      case ALREADY_INSTALLED:
        return true;
      default:
        return false;
    }
  }
}
