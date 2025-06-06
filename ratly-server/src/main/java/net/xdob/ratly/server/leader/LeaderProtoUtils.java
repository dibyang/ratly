package net.xdob.ratly.server.leader;

import net.xdob.ratly.client.impl.ClientProtoUtils;
import net.xdob.ratly.proto.raft.FileChunkProto;
import net.xdob.ratly.proto.raft.InstallSnapshotRequestProto;
import net.xdob.ratly.proto.raft.InstallSnapshotRequestProto.NotificationProto;
import net.xdob.ratly.proto.raft.InstallSnapshotRequestProto.SnapshotChunkProto;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.Division;
import net.xdob.ratly.server.RaftConfiguration;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.LogProtoUtils;

import java.util.Collections;

/** Leader only proto utilities. */
final class LeaderProtoUtils {
  private LeaderProtoUtils() {}

  static SnapshotChunkProto.Builder toSnapshotChunkProtoBuilder(String requestId, int requestIndex,
      TermIndex lastTermIndex, FileChunkProto chunk, long totalSize, boolean done) {
    return SnapshotChunkProto.newBuilder()
        .setRequestId(requestId)
        .setRequestIndex(requestIndex)
        .setTermIndex(lastTermIndex.toProto())
        .addAllFileChunks(Collections.singleton(chunk))
        .setTotalSize(totalSize)
        .setDone(done);
  }

  static InstallSnapshotRequestProto toInstallSnapshotRequestProto(
      Division server, RaftPeerId replyId, SnapshotChunkProto.Builder chunk) {
    return toInstallSnapshotRequestProtoBuilder(server, replyId)
        .setSnapshotChunk(chunk)
        .build();
  }

  static InstallSnapshotRequestProto toInstallSnapshotRequestProto(
      Division server, RaftPeerId replyId, TermIndex firstAvailable) {
    return toInstallSnapshotRequestProtoBuilder(server, replyId)
        .setNotification(NotificationProto.newBuilder().setFirstAvailableTermIndex(firstAvailable.toProto()))
        .build();
  }

  private static InstallSnapshotRequestProto.Builder toInstallSnapshotRequestProtoBuilder(
      Division server, RaftPeerId replyId) {
    // term is not going to used by installSnapshot to update the RaftConfiguration
    final RaftConfiguration conf = server.getRaftConf();
    final LogEntryProto confLogEntryProto = LogProtoUtils.toLogEntryProto(conf, null, conf.getLogEntryIndex());
    return InstallSnapshotRequestProto.newBuilder()
        .setServerRequest(ClientProtoUtils.toRaftRpcRequestProtoBuilder(server.getMemberId(), replyId))
        .setLeaderTerm(server.getInfo().getCurrentTerm())
        .setLastRaftConfigurationLogEntryProto(confLogEntryProto);
  }
}
