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
 *  一个用于将 Protocol Buffers 消息对象转换为简洁、紧凑的字符串表示的工具类。
 *  它提供了一些静态方法，将不同类型的 Raft 协议消息（如 AppendEntriesRequestProto、
 *  InstallSnapshotRequestProto 等）转换为易于阅读和调试的字符串格式
 * <p>
 *  目的：
 * 该类主要用于将 Protocol Buffers 消息转换为易于阅读的字符串。
 * 它们并不是标准的 toString() 方法输出，而是根据特定格式生成的字符串，通常用于日志记录或调试信息显示。
 * <p>
 * 注意：
 * 输出的字符串只是信息性字符串，不能用于其他目的，如解析或反序列化。
 * 如果需要处理消息，应该使用 Protocol Buffers 提供的 API。
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
