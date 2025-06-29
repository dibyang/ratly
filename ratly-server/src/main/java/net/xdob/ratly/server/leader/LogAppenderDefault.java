package net.xdob.ratly.server.leader;

import net.xdob.ratly.proto.raft.AppendEntriesReplyProto;
import net.xdob.ratly.proto.raft.AppendEntriesRequestProto;
import net.xdob.ratly.proto.raft.InstallSnapshotReplyProto;
import net.xdob.ratly.proto.raft.InstallSnapshotRequestProto;
import net.xdob.ratly.server.Division;
import net.xdob.ratly.server.protocol.RaftServerProtocol;
import net.xdob.ratly.rpc.CallId;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.raftlog.RaftLogIOException;
import net.xdob.ratly.server.util.ServerStringUtils;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.util.ReferenceCountedObject;
import net.xdob.ratly.util.Timestamp;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 本地日志追加器
 * The default implementation of {@link LogAppender}
 * using {@link RaftServerProtocol}.
 */
class LogAppenderDefault extends LogAppenderBase {
  LogAppenderDefault(Division server, LeaderState leaderState, FollowerInfo f) {
    super(server, leaderState, f);
  }

  @Override
  public long getCallId() {
    return CallId.get();
  }

  @Override
  public Comparator<Long> getCallIdComparator() {
    return CallId.getComparator();
  }

  /** Send an appendEntries RPC; retry indefinitely. */
  private AppendEntriesReplyProto sendAppendEntriesWithRetries(AtomicLong requestFirstIndex)
      throws InterruptedException, InterruptedIOException, RaftLogIOException {
    for(int retry = 0; isRunning(); retry++) {
      final ReferenceCountedObject<AppendEntriesRequestProto> request = nextAppendEntriesRequest(
          CallId.getAndIncrement(), false);
      if (request == null) {
        LOG.trace("{} no entries to send now, wait ...", this);
        return null;
      }
      try {
        if (!isRunning()) {
          LOG.info("{} is stopped. Skip appendEntries.", this);
          return null;
        }

        final AppendEntriesRequestProto proto = request.get();
        final AppendEntriesReplyProto reply = sendAppendEntries(proto);
        final long first = proto.getEntriesCount() > 0 ? proto.getEntries(0).getIndex() : RaftLog.INVALID_LOG_INDEX;
        requestFirstIndex.set(first);
        return reply;
      } catch (InterruptedIOException | RaftLogIOException e) {
        throw e;
      } catch (IOException ioe) {
        // TODO should have more detailed retry policy here.
        if (retry % 10 == 0) { // to reduce the number of messages
          LOG.warn("{}: Failed to appendEntries (retry={})", this, retry, ioe);
        }
        handleException(ioe);
      } finally {
        request.release();
      }

      if (isRunning()) {
        getServer().properties().rpcSleepTime().sleep();
      }
    }
    return null;
  }

  private AppendEntriesReplyProto sendAppendEntries(AppendEntriesRequestProto request) throws IOException {
    resetHeartbeatTrigger();
    final Timestamp sendTime = Timestamp.currentTime();
    getFollower().updateLastRpcSendTime(request.getEntriesCount() == 0);
    final AppendEntriesReplyProto r = getServerRpc().appendEntries(request);
    getFollower().updateLastRpcResponseTime();
    getFollower().updateLastRespondedAppendEntriesSendTime(sendTime);

    getLeaderState().onFollowerCommitIndex(getFollower(), r.getFollowerCommit());
    return r;
  }

  private InstallSnapshotReplyProto installSnapshot(SnapshotInfo snapshot) throws InterruptedIOException {
    String requestId = UUID.randomUUID().toString();
    InstallSnapshotReplyProto reply = null;
    try {
      for (InstallSnapshotRequestProto request : newInstallSnapshotRequests(requestId, snapshot)) {
        getFollower().updateLastRpcSendTime(false);
        reply = getServerRpc().installSnapshot(request);
        getFollower().updateLastRpcResponseTime();

        if (!reply.getServerReply().getSuccess()) {
          return reply;
        }
      }
    } catch (InterruptedIOException iioe) {
      throw iioe;
    } catch (Exception ioe) {
      LOG.warn("{}: Failed to installSnapshot {}", this, snapshot, ioe);
      handleException(ioe);
      return null;
    }

    if (reply != null) {
      getFollower().setSnapshotIndex(snapshot.getTermIndex().getIndex());
      LOG.info("{}: installSnapshot {} successfully", this, snapshot);
      getServer().getRaftServerMetrics().onSnapshotInstalled();
    }
    return reply;
  }

  @Override
  public void run() throws InterruptedException, IOException {
    while (isRunning()) {
      if (shouldSendAppendEntries()) {
        SnapshotInfo snapshot = shouldInstallSnapshot();
        if (snapshot != null) {
          LOG.info("{}: followerNextIndex = {} but logStartIndex = {}, send snapshot {} to follower",
              this, getFollower().getNextIndex(), getRaftLog().getStartIndex(), snapshot);

          final InstallSnapshotReplyProto r = installSnapshot(snapshot);
          if (r != null) {
            switch (r.getResult()) {
              case NOT_LEADER:
                onFollowerTerm(r.getTerm());
                break;
              case SUCCESS:
              case SNAPSHOT_UNAVAILABLE:
              case ALREADY_INSTALLED:
              case SNAPSHOT_EXPIRED:
                getFollower().setAttemptedToInstallSnapshot();
                break;
              default:
                break;
            }
          }
          // otherwise if r is null, retry the snapshot installation
        } else {
          final AtomicLong requestFirstIndex = new AtomicLong(RaftLog.INVALID_LOG_INDEX);
          final AppendEntriesReplyProto r = sendAppendEntriesWithRetries(requestFirstIndex);
          if (r != null) {
            handleReply(r, requestFirstIndex.get());
          }
        }
      }
      if (isRunning() && !hasAppendEntries()) {
        getEventAwaitForSignal().await(getHeartbeatWaitTimeMs(), TimeUnit.MILLISECONDS);
      }
      getLeaderState().checkHealth(getFollower());
    }
  }

  private void handleReply(AppendEntriesReplyProto reply, long requestFirstIndex)
      throws IllegalArgumentException {
    if (reply != null) {
      switch (reply.getResult()) {
        case SUCCESS:
          final long oldNextIndex = getFollower().getNextIndex();
          final long nextIndex = reply.getNextIndex();
          if (nextIndex < oldNextIndex) {
            throw new IllegalStateException("nextIndex=" + nextIndex
                + " < oldNextIndex=" + oldNextIndex
                + ", reply=" + ServerStringUtils.toAppendEntriesReplyString(reply));
          }

          if (nextIndex > oldNextIndex) {
            getFollower().updateMatchIndex(nextIndex - 1);
            getFollower().increaseNextIndex(nextIndex);
            getLeaderState().onFollowerSuccessAppendEntries(getFollower());
          }
          break;
        case NOT_LEADER:
          // check if should step down
          onFollowerTerm(reply.getTerm());
          break;
        case INCONSISTENCY:
          getFollower().setNextIndex(getNextIndexForInconsistency(requestFirstIndex, reply.getNextIndex()));
          break;
        case UNRECOGNIZED:
          LOG.warn("{}: received {}", this, reply.getResult());
          break;
        default: throw new IllegalArgumentException("Unable to process result " + reply.getResult());
      }
      getLeaderState().onAppendEntriesReply(this, reply);
    }
  }

  private void handleException(Exception e) {
    LOG.trace("TRACE", e);
    getServerRpc().handleException(getFollowerId(), e, false);
  }
}
