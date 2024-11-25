
package net.xdob.ratly.server.util;

import net.xdob.ratly.proto.raft.AppendEntriesReplyProto;
import net.xdob.ratly.proto.raft.AppendEntriesRequestProto;
import net.xdob.ratly.proto.raft.InstallSnapshotReplyProto;
import net.xdob.ratly.proto.raft.InstallSnapshotRequestProto;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.proto.raft.RequestVoteReplyProto;
import net.xdob.ratly.proto.raft.StateMachineLogEntryProto;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.LogProtoUtils;
import net.xdob.ratly.util.ProtoUtils;

import java.util.List;
import java.util.function.Function;

/**
 *  This class provides convenient utilities for converting Protocol Buffers messages to strings.
 *  The output strings are for information purpose only.
 *  They are concise and compact compared to the Protocol Buffers implementations of {@link Object#toString()}.
 * <p>
 *  The output messages or the output formats may be changed without notice.
 *  Callers of this class should not try to parse the output strings for any purposes.
 *  Instead, they should use the public APIs provided by Protocol Buffers.
 */
public final class ServerStringUtils {
  private ServerStringUtils() {}

  public static String toAppendEntriesRequestString(AppendEntriesRequestProto request,
      Function<StateMachineLogEntryProto, String> stateMachineToString) {
    if (request == null) {
      return null;
    }
    final List<LogEntryProto> entries = request.getEntriesList();
    return ProtoUtils.toString(request.getServerRequest())
        + "-t" + request.getLeaderTerm()
        + ",previous=" + TermIndex.valueOf(request.getPreviousLog())
        + ",leaderCommit=" + request.getLeaderCommit()
        + ",initializing? " + request.getInitializing()
        + "," + (entries.isEmpty()? "HEARTBEAT" : "entries: " +
        LogProtoUtils.toLogEntriesShortString(entries, stateMachineToString));
  }

  public static String toAppendEntriesReplyString(AppendEntriesReplyProto reply) {
    if (reply == null) {
      return null;
    }
    return ProtoUtils.toString(reply.getServerReply())
        + "-t" + reply.getTerm()
        + "," + reply.getResult()
        + ",nextIndex=" + reply.getNextIndex()
        + ",followerCommit=" + reply.getFollowerCommit()
        + ",matchIndex=" + reply.getMatchIndex();
  }

  public static String toInstallSnapshotRequestString(InstallSnapshotRequestProto request) {
    if (request == null) {
      return null;
    }
    final String s;
    switch (request.getInstallSnapshotRequestBodyCase()) {
      case SNAPSHOTCHUNK:
        final InstallSnapshotRequestProto.SnapshotChunkProto chunk = request.getSnapshotChunk();
        s = "chunk:" + chunk.getRequestId() + "," + chunk.getRequestIndex();
        break;
      case NOTIFICATION:
        final InstallSnapshotRequestProto.NotificationProto notification = request.getNotification();
        s = "notify:" + TermIndex.valueOf(notification.getFirstAvailableTermIndex());
        break;
      default:
        throw new IllegalStateException("Unexpected body case in " + request);
    }
    return ProtoUtils.toString(request.getServerRequest())
        + "-t" + request.getLeaderTerm()
        + "," + s;
  }

  public static String toInstallSnapshotReplyString(InstallSnapshotReplyProto reply) {
    if (reply == null) {
      return null;
    }
    final String s;
    switch (reply.getInstallSnapshotReplyBodyCase()) {
      case REQUESTINDEX:
        s = ",requestIndex=" + reply.getRequestIndex();
        break;
      case SNAPSHOTINDEX:
        s = ",snapshotIndex=" + reply.getSnapshotIndex();
        break;
      default:
        s = ""; // result is not SUCCESS
    }
    return ProtoUtils.toString(reply.getServerReply())
        + "-t" + reply.getTerm()
        + "," + reply.getResult() + s;
  }

  public static String toRequestVoteReplyString(RequestVoteReplyProto proto) {
    if (proto == null) {
      return null;
    }
    return ProtoUtils.toString(proto.getServerReply()) + "-t" + proto.getTerm();
  }
}
