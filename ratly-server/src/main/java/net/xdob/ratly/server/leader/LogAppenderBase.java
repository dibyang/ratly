
package net.xdob.ratly.server.leader;

import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.proto.raft.AppendEntriesRequestProto;
import net.xdob.ratly.proto.raft.InstallSnapshotRequestProto;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.RaftServerConfigKeys;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.raftlog.RaftLog.EntryWithData;
import net.xdob.ratly.server.raftlog.RaftLogIOException;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.util.AwaitForSignal;
import net.xdob.ratly.util.DataQueue;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.LifeCycle;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.ReferenceCountedObject;
import net.xdob.ratly.util.SizeInBytes;
import net.xdob.ratly.util.TimeDuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongUnaryOperator;

/**
 * An abstract implementation of {@link LogAppender}.
 */
public abstract class LogAppenderBase implements LogAppender {
  private final String name;
  private final RaftServer.Division server;
  private final LeaderState leaderState;
  private final FollowerInfo follower;

  private final DataQueue<EntryWithData> buffer;
  private final int snapshotChunkMaxSize;

  private final LogAppenderDaemon daemon;
  private final AwaitForSignal eventAwaitForSignal;

  private final AtomicBoolean heartbeatTrigger = new AtomicBoolean();
  private final TimeDuration waitTimeMin;

  protected LogAppenderBase(RaftServer.Division server, LeaderState leaderState, FollowerInfo f) {
    this.follower = f;
    this.name = follower.getName() + "-" + JavaUtils.getClassSimpleName(getClass());
    this.server = server;
    this.leaderState = leaderState;

    final RaftProperties properties = server.getRaftServer().getProperties();
    this.snapshotChunkMaxSize = RaftServerConfigKeys.Log.Appender.snapshotChunkSizeMax(properties).getSizeInt();

    final SizeInBytes bufferByteLimit = RaftServerConfigKeys.Log.Appender.bufferByteLimit(properties);
    final int bufferElementLimit = RaftServerConfigKeys.Log.Appender.bufferElementLimit(properties);
    this.buffer = new DataQueue<>(this, bufferByteLimit, bufferElementLimit, EntryWithData::getSerializedSize);
    this.daemon = new LogAppenderDaemon(this);
    this.eventAwaitForSignal = new AwaitForSignal(name);

    this.waitTimeMin = RaftServerConfigKeys.Log.Appender.waitTimeMin(properties);
  }

  @Override
  public void triggerHeartbeat() {
    if (heartbeatTrigger.compareAndSet(false, true)) {
      notifyLogAppender();
    }
  }

  protected void resetHeartbeatTrigger() {
    heartbeatTrigger.set(false);
  }

  @Override
  public boolean shouldSendAppendEntries() {
    return heartbeatTrigger.get() || LogAppender.super.shouldSendAppendEntries();
  }

  @Override
  public long getHeartbeatWaitTimeMs() {
    return heartbeatTrigger.get() ? 0 :
        LogAppender.super.getHeartbeatWaitTimeMs();
  }

  @Override
  public AwaitForSignal getEventAwaitForSignal() {
    return eventAwaitForSignal;
  }

  @Override
  public final RaftServer.Division getServer() {
    return server;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public void start() {
    daemon.tryToStart();
  }

  @Override
  public boolean isRunning() {
    return daemon.isWorking() && server.getInfo().isLeader();
  }

  @Override
  public CompletableFuture<LifeCycle.State> stopAsync() {
    return daemon.tryToClose();
  }

  void restart() {
    if (!server.getInfo().isAlive()) {
      LOG.warn("Failed to restart {}: server {} is not alive", this, server.getMemberId());
      return;
    }
    getLeaderState().restart(this);
  }

  protected TimeDuration getWaitTimeMin() {
    return waitTimeMin;
  }

  protected TimeDuration getRemainingWaitTime() {
    return waitTimeMin.add(getFollower().getLastRpcSendTime().elapsedTime().negate());
  }

  @Override
  public final FollowerInfo getFollower() {
    return follower;
  }

  @Override
  public final LeaderState getLeaderState() {
    return leaderState;
  }

  public boolean hasPendingDataRequests() {
    return false;
  }

  private TermIndex getPrevious(long nextIndex) {
    if (nextIndex == RaftLog.LEAST_VALID_LOG_INDEX) {
      return null;
    }

    final long previousIndex = nextIndex - 1;
    final TermIndex previous = getRaftLog().getTermIndex(previousIndex);
    if (previous != null) {
      return previous;
    }

    final SnapshotInfo snapshot = server.getStateMachine().getLatestSnapshot();
    if (snapshot != null) {
      final TermIndex snapshotTermIndex = snapshot.getTermIndex();
      if (snapshotTermIndex.getIndex() == previousIndex) {
        return snapshotTermIndex;
      }
    }

    return null;
  }

  protected long getNextIndexForInconsistency(long requestFirstIndex, long replyNextIndex) {
    long next = replyNextIndex;
    final long i = getFollower().getMatchIndex() + 1;
    if (i > next && i != requestFirstIndex) {
      // Ideally, we should set nextIndex to a value greater than matchIndex.
      // However, we must not resend the same first entry due to some special cases (e.g. the log is empty).
      // Otherwise, the follower will reply INCONSISTENCY again.
      next = i;
    }
    if (next == requestFirstIndex && next > RaftLog.LEAST_VALID_LOG_INDEX) {
      // Avoid resending the same first entry.
      next--;
    }
    return next;
  }

  protected LongUnaryOperator getNextIndexForError(long newNextIndex) {
    return oldNextIndex -> {
      final long m = getFollower().getMatchIndex() + 1;
      final long n = oldNextIndex <= 0L ? oldNextIndex : Math.min(oldNextIndex - 1, newNextIndex);
      if (m > n) {
        if (m > newNextIndex) {
          LOG.info("Set nextIndex to matchIndex + 1 (= " + m + ")");
        }
        return m;
      } else if (oldNextIndex <= 0L) {
        return oldNextIndex; // no change.
      } else {
        LOG.info("Decrease nextIndex to " + n);
        return n;
      }
    };
  }

  @Override
  public AppendEntriesRequestProto newAppendEntriesRequest(long callId, boolean heartbeat) {
    throw new UnsupportedOperationException("Use nextAppendEntriesRequest(" + callId + ", " + heartbeat +") instead.");
  }

/**
 * Create a {@link AppendEntriesRequestProto} object using the {@link FollowerInfo} of this {@link LogAppender}.
 * The {@link AppendEntriesRequestProto} object may contain zero or more log entries.
 * When there is zero log entries, the {@link AppendEntriesRequestProto} object is a heartbeat.
 *
 * @param callId The call id of the returned request.
 * @param heartbeat the returned request must be a heartbeat.
 *
 * @return a retained reference of {@link AppendEntriesRequestProto} object.
 *         Since the returned reference is retained, the caller must call {@link ReferenceCountedObject#release()}}
 *         after use.
 */
  protected ReferenceCountedObject<AppendEntriesRequestProto> nextAppendEntriesRequest(long callId, boolean heartbeat)
      throws RaftLogIOException {
    final long heartbeatWaitTimeMs = getHeartbeatWaitTimeMs();
    final TermIndex previous = getPrevious(follower.getNextIndex());
    if (heartbeatWaitTimeMs <= 0L || heartbeat) {
      // heartbeat
      AppendEntriesRequestProto heartbeatRequest =
          leaderState.newAppendEntriesRequestProto(follower, Collections.emptyList(),
              hasPendingDataRequests() ? null : previous, callId);
      ReferenceCountedObject<AppendEntriesRequestProto> ref = ReferenceCountedObject.wrap(heartbeatRequest);
      ref.retain();
      return ref;
    }

    Preconditions.assertTrue(buffer.isEmpty(), () -> "buffer has " + buffer.getNumElements() + " elements.");

    final long snapshotIndex = follower.getSnapshotIndex();
    final long leaderNext = getRaftLog().getNextIndex();
    final long followerNext = follower.getNextIndex();
    final long halfMs = heartbeatWaitTimeMs/2;
    final Map<Long, ReferenceCountedObject<EntryWithData>> offered = new HashMap<>();
    for (long next = followerNext; leaderNext > next && getHeartbeatWaitTimeMs() - halfMs > 0; next++) {
      final ReferenceCountedObject<EntryWithData> entryWithData = getRaftLog().retainEntryWithData(next);
      if (!buffer.offer(entryWithData.get())) {
        entryWithData.release();
        break;
      }
      offered.put(next, entryWithData);
    }
    if (buffer.isEmpty()) {
      return null;
    }

    final List<LogEntryProto> protos;
    try {
      protos = buffer.pollList(getHeartbeatWaitTimeMs(), EntryWithData::getEntry,
          (entry, time, exception) -> LOG.warn("Failed to get {} in {}",
              entry, time.toString(TimeUnit.MILLISECONDS, 3), exception));
    } catch (RaftLogIOException e) {
      for (ReferenceCountedObject<EntryWithData> ref : offered.values()) {
        ref.release();
      }
      offered.clear();
      throw e;
    } finally {
      for (EntryWithData entry : buffer) {
        // Release remaining entries.
        Optional.ofNullable(offered.remove(entry.getIndex())).ifPresent(ReferenceCountedObject::release);
      }
      buffer.clear();
    }
    assertProtos(protos, followerNext, previous, snapshotIndex);
    AppendEntriesRequestProto appendEntriesProto =
        leaderState.newAppendEntriesRequestProto(follower, protos, previous, callId);
    return ReferenceCountedObject.delegateFrom(offered.values(), appendEntriesProto);
  }

  private void assertProtos(List<LogEntryProto> protos, long nextIndex, TermIndex previous, long snapshotIndex) {
    if (protos.isEmpty()) {
      return;
    }
    final long firstIndex = protos.get(0).getIndex();
    Preconditions.assertTrue(firstIndex == nextIndex,
        () -> follower.getName() + ": firstIndex = " + firstIndex + " != nextIndex = " + nextIndex);
    if (firstIndex > RaftLog.LEAST_VALID_LOG_INDEX) {
      // Check if nextIndex is 1 greater than the snapshotIndex. If yes, then
      // we do not have to check for the existence of previous.
      if (nextIndex != snapshotIndex + 1) {
        Objects.requireNonNull(previous,
            () -> follower.getName() + ": Previous TermIndex not found for firstIndex = " + firstIndex);
        Preconditions.assertTrue(previous.getIndex() == firstIndex - 1,
            () -> follower.getName() + ": Previous = " + previous + " but firstIndex = " + firstIndex);
      }
    }
  }

  @Override
  public InstallSnapshotRequestProto newInstallSnapshotNotificationRequest(TermIndex firstAvailableLogTermIndex) {
    Preconditions.assertTrue(firstAvailableLogTermIndex.getIndex() >= 0);
    synchronized (server) {
      return LeaderProtoUtils.toInstallSnapshotRequestProto(server, getFollowerId(), firstAvailableLogTermIndex);
    }
  }

  @Override
  public Iterable<InstallSnapshotRequestProto> newInstallSnapshotRequests(String requestId, SnapshotInfo snapshot) {
    return new InstallSnapshotRequests(server, getFollowerId(), requestId, snapshot, snapshotChunkMaxSize);
  }
}
