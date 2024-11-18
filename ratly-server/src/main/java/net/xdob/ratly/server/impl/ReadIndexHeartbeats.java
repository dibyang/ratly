
package net.xdob.ratly.server.impl;

import net.xdob.ratly.proto.RaftProtos.AppendEntriesReplyProto;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.leader.LogAppender;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.raftlog.RaftLogIndex;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

class ReadIndexHeartbeats {
  private static final Logger LOG = LoggerFactory.getLogger(ReadIndexHeartbeats.class);

  /** The acknowledgement from a {@link LogAppender} of a heartbeat for a particular call id. */
  static class HeartbeatAck {
    private final LogAppender appender;
    private final long minCallId;
    private volatile boolean acknowledged = false;

    HeartbeatAck(LogAppender appender) {
      this.appender = appender;
      this.minCallId = appender.getCallId();
    }

    /** Is the heartbeat (for a particular call id) acknowledged? */
    boolean isAcknowledged() {
      return acknowledged;
    }

    /**
     * @return true if the acknowledged state is changed from false to true;
     *         otherwise, the acknowledged state remains unchanged, return false.
     */
    boolean receive(AppendEntriesReplyProto reply) {
      if (acknowledged) {
        return false;
      }
      synchronized (this) {
        if (!acknowledged && isValid(reply)) {
          acknowledged = true;
          return true;
        }
        return false;
      }
    }

    private boolean isValid(AppendEntriesReplyProto reply) {
      if (reply == null || !reply.getServerReply().getSuccess()) {
        return false;
      }
      // valid only if the reply has a later call id than the min.
      return appender.getCallIdComparator().compare(reply.getServerReply().getCallId(), minCallId) >= 0;
    }
  }

  static class AppendEntriesListener {
    private final long commitIndex;
    private final CompletableFuture<Long> future = new CompletableFuture<>();
    private final ConcurrentHashMap<RaftPeerId, HeartbeatAck> replies = new ConcurrentHashMap<>();

    AppendEntriesListener(long commitIndex, Iterable<LogAppender> logAppenders) {
      this.commitIndex = commitIndex;
      for (LogAppender a : logAppenders) {
        a.triggerHeartbeat();
        replies.put(a.getFollowerId(), new HeartbeatAck(a));
      }
    }

    CompletableFuture<Long> getFuture() {
      return future;
    }

    boolean receive(LogAppender logAppender, AppendEntriesReplyProto proto,
                    Predicate<Predicate<RaftPeerId>> hasMajority) {
      if (JavaUtils.isCompletedNormally(future)) {
        return true;
      }

      final HeartbeatAck reply = replies.computeIfAbsent(
          logAppender.getFollowerId(), key -> new HeartbeatAck(logAppender));
      if (reply.receive(proto)) {
        if (hasMajority.test(this::isAcknowledged)) {
          future.complete(commitIndex);
          return true;
        }
      }

      return JavaUtils.isCompletedNormally(future);
    }

    boolean isAcknowledged(RaftPeerId id) {
      return Optional.ofNullable(replies.get(id)).filter(HeartbeatAck::isAcknowledged).isPresent();
    }
  }

  class AppendEntriesListeners {
    private final NavigableMap<Long, AppendEntriesListener> sorted = new TreeMap<>();
    private Exception exception = null;

    synchronized AppendEntriesListener add(long commitIndex, Function<Long, AppendEntriesListener> constructor) {
      if (exception != null) {
        Preconditions.assertTrue(sorted.isEmpty());
        final AppendEntriesListener listener = constructor.apply(commitIndex);
        listener.getFuture().completeExceptionally(exception);
        return listener;
      }
      return sorted.computeIfAbsent(commitIndex, constructor);
    }

    synchronized void onAppendEntriesReply(LogAppender appender, AppendEntriesReplyProto reply,
                                           Predicate<Predicate<RaftPeerId>> hasMajority) {
      final long followerCommit = reply.getFollowerCommit();

      Iterator<Map.Entry<Long, AppendEntriesListener>> iterator = sorted.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Long, AppendEntriesListener> entry = iterator.next();
        if (entry.getKey() > followerCommit) {
          return;
        }

        final AppendEntriesListener listener = entry.getValue();
        if (listener == null) {
          continue;
        }

        if (listener.receive(appender, reply, hasMajority)) {
          ackedCommitIndex.updateToMax(listener.commitIndex, s -> LOG.debug("{}: {}", this, s));
          iterator.remove();
        }
      }
    }

    synchronized void failAll(Exception e) {
      if (exception != null) {
        return;
      }
      exception = e;
      sorted.forEach((index, listener) -> listener.getFuture().completeExceptionally(e));
      sorted.clear();
    }
  }

  private final AppendEntriesListeners appendEntriesListeners = new AppendEntriesListeners();
  private final RaftLogIndex ackedCommitIndex = new RaftLogIndex("ackedCommitIndex", RaftLog.INVALID_LOG_INDEX);

  AppendEntriesListener addAppendEntriesListener(long commitIndex, Function<Long, AppendEntriesListener> constructor) {
    if (commitIndex <= ackedCommitIndex.get()) {
      return null;
    }
    LOG.debug("listen commitIndex {}", commitIndex);
    return appendEntriesListeners.add(commitIndex, constructor);
  }

  void onAppendEntriesReply(LogAppender appender, AppendEntriesReplyProto reply,
                            Predicate<Predicate<RaftPeerId>> hasMajority) {
    appendEntriesListeners.onAppendEntriesReply(appender, reply, hasMajority);
  }

  void failListeners(Exception e) {
    appendEntriesListeners.failAll(e);
  }
}
