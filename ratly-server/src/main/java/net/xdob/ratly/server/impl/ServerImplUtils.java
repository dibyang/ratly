package net.xdob.ratly.server.impl;

import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.proto.raft.AppendEntriesRequestProto;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.protocol.RaftClientRequest;
import net.xdob.ratly.protocol.RaftGroup;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftGroupMemberId;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.protocol.exceptions.GroupMismatchException;
import net.xdob.ratly.server.RaftConfiguration;
import net.xdob.ratly.server.RaftConfigurationImpl;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.storage.StartupOption;
import net.xdob.ratly.statemachine.StateMachine;
import net.xdob.ratly.util.IOUtils;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.TimeDuration;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Server utilities for internal use. */
public final class ServerImplUtils {
  private ServerImplUtils() {
    //Never constructed
  }

  /** Create a {@link RaftServerProxy}. */
  public static RaftServerProxy newRaftServer(
      RaftPeerId id, RaftGroup group, StartupOption option, StateMachine.Registry stateMachineRegistry,
      ThreadGroup threadGroup, RaftProperties properties, Parameters parameters) throws IOException {
    RaftServer.LOG.debug("newRaftServer: {}, {}", id, group);
    if (group != null && !group.getPeers().isEmpty()) {
      Preconditions.assertNotNull(id, "RaftPeerId %s is not in RaftGroup %s", id, group);
      Preconditions.assertNotNull(group.getPeer(id), "RaftPeerId %s is not in RaftGroup %s", id, group);
    }
    final RaftServerProxy proxy = newRaftServer(id, stateMachineRegistry, threadGroup, properties, parameters);
    proxy.initGroups(group, option);
    return proxy;
  }

  private static RaftServerProxy newRaftServer(
      RaftPeerId id, StateMachine.Registry stateMachineRegistry, ThreadGroup threadGroup, RaftProperties properties,
      Parameters parameters) throws IOException {
    final TimeDuration sleepTime = TimeDuration.valueOf(500, TimeUnit.MILLISECONDS);
    final RaftServerProxy proxy;
    try {
      // attempt multiple times to avoid temporary bind exception
      proxy = JavaUtils.attemptRepeatedly(
          () -> new RaftServerProxy(id, stateMachineRegistry, properties, parameters, threadGroup),
          5, sleepTime, "new RaftServerProxy", RaftServer.LOG);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw IOUtils.toInterruptedIOException(
          "Interrupted when creating RaftServer " + id, e);
    }
    return proxy;
  }

  public static RaftConfiguration newRaftConfiguration(List<RaftPeer> conf, List<RaftPeer> listener,
      long index, List<RaftPeer> oldConf, List<RaftPeer> oldListener) {
    final RaftConfigurationImpl.Builder b = RaftConfigurationImpl.newBuilder()
        .setConf(conf, listener)
        .setLogEntryIndex(index);
    if (!oldConf.isEmpty() || !oldListener.isEmpty()) {
      b.setOldConf(oldConf, oldListener);
    }
    return b.build();
  }

  static long effectiveCommitIndex(long leaderCommitIndex, TermIndex followerPrevious, int numAppendEntries) {
    final long previous = followerPrevious != null? followerPrevious.getIndex() : RaftLog.LEAST_VALID_LOG_INDEX;
    return Math.min(leaderCommitIndex, previous + numAppendEntries);
  }

  static void assertGroup(RaftGroupMemberId serverMemberId, RaftClientRequest request) throws GroupMismatchException {
    assertGroup(serverMemberId, request.getRequestorId(), request.getRaftGroupId());
  }

  static void assertGroup(RaftGroupMemberId localMemberId, Object remoteId, RaftGroupId remoteGroupId)
      throws GroupMismatchException {
    final RaftGroupId localGroupId = localMemberId.getGroupId();
    if (!localGroupId.equals(remoteGroupId)) {
      throw new GroupMismatchException(localMemberId
          + ": The group (" + remoteGroupId + ") of remote " + remoteId
          + " does not match the group (" + localGroupId + ") of local " + localMemberId.getPeerId());
    }
  }

  static void assertEntries(AppendEntriesRequestProto proto, TermIndex previous, ServerState state) {
    final List<LogEntryProto> entries = proto.getEntriesList();
    if (!entries.isEmpty()) {
      final long index0 = entries.get(0).getIndex();
      // Check if next entry's index is 1 greater than the snapshotIndex. If yes, then
      // we do not have to check for the existence of previous.
      if (index0 != state.getSnapshotIndex() + 1) {
        final long expected = previous == null || previous.getTerm() == 0 ? 0 : previous.getIndex() + 1;
        Preconditions.assertTrue(index0 == expected,
            "Unexpected Index: previous is %s but entries[%s].getIndex() == %s != %s",
            previous, 0, index0, expected);
      }

      final long leaderTerm = proto.getLeaderTerm();
      for (int i = 0; i < entries.size(); i++) {
        final LogEntryProto entry = entries.get(i);
        final long entryTerm = entry.getTerm();
        Preconditions.assertTrue(entryTerm <= leaderTerm ,
            "Unexpected Term: entries[%s].getTerm() == %s > leaderTerm == %s",
            i, entryTerm, leaderTerm);

        final long indexI = entry.getIndex();
        final long expected = index0 + i;
        Preconditions.assertTrue(indexI == expected,
            "Unexpected Index: entries[0].getIndex() == %s but entries[%s].getIndex() == %s != %s",
            index0, i, indexI, expected);
      }
    }
  }
}
