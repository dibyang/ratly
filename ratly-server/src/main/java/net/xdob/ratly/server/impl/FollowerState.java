package net.xdob.ratly.server.impl;

import net.xdob.ratly.server.DivisionInfo;
import net.xdob.ratly.server.leader.LeaderState;
import net.xdob.ratly.util.Daemon;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.TimeDuration;
import net.xdob.ratly.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToIntFunction;

/**
 * Used when the peer is a follower. Used to track the election timeout.
 */
class FollowerState extends Daemon {
  enum UpdateType {
    APPEND_START(AtomicInteger::incrementAndGet),
    APPEND_COMPLETE(AtomicInteger::decrementAndGet),
    INSTALL_SNAPSHOT_START(AtomicInteger::incrementAndGet),
    INSTALL_SNAPSHOT_COMPLETE(AtomicInteger::decrementAndGet),
    INSTALL_SNAPSHOT_NOTIFICATION(AtomicInteger::get),
    REQUEST_VOTE(AtomicInteger::get);

    private final ToIntFunction<AtomicInteger> updateFunction;

    UpdateType(ToIntFunction<AtomicInteger> updateFunction) {
      this.updateFunction = updateFunction;
    }

    int update(AtomicInteger outstanding) {
      return updateFunction.applyAsInt(outstanding);
    }
  }

  static final Logger LOG = LoggerFactory.getLogger(FollowerState.class);

  private final Object reason;
  private final RaftServerImpl server;

  private final Timestamp creationTime = Timestamp.currentTime();
  @SuppressWarnings({"squid:S3077"}) // Suppress volatile for generic type
  private volatile Timestamp lastRpcTime = creationTime;
  private volatile boolean isRunning = true;
  private final CompletableFuture<Void> stopped = new CompletableFuture<>();
  private final AtomicInteger outstandingOp = new AtomicInteger();

  FollowerState(RaftServerImpl server, Object reason) {
    super(newBuilder()
        .setName(server.getMemberId() + "-" + JavaUtils.getClassSimpleName(FollowerState.class))
        .setThreadGroup(server.getThreadGroup()));
    this.server = server;
    this.reason = reason;
  }

  void updateLastRpcTime(UpdateType type) {
    lastRpcTime = Timestamp.currentTime();

    final int n = type.update(outstandingOp);
    if (LOG.isTraceEnabled()) {
      LOG.trace("{}: update lastRpcTime to {} for {}, outstandingOp={}", this, lastRpcTime, type, n);
    }
  }

  Timestamp getLastRpcTime() {
    return lastRpcTime;
  }

  int getOutstandingOp() {
    return outstandingOp.get();
  }

  boolean isCurrentLeaderValid() {
    return lastRpcTime.elapsedTime().compareTo(server.properties().minRpcTimeout()) < 0;
  }

  CompletableFuture<Void> stopRunning() {
    this.isRunning = false;
    interrupt();
    return stopped;
  }

  boolean lostMajorityHeartbeatsRecently() {
    if (reason != LeaderState.StepDownReason.LOST_MAJORITY_HEARTBEATS) {
      return false;
    }
    final TimeDuration elapsed = creationTime.elapsedTime();
    final TimeDuration waitTime = server.getLeaderStepDownWaitTime();
    if (elapsed.compareTo(waitTime) >= 0) {
      return false;
    }
    LOG.info("{}: Skipping leader election since it stepped down recently (elapsed = {} < waitTime = {})",
        this, elapsed.to(TimeUnit.MILLISECONDS), waitTime);
    return true;
  }

  private boolean shouldRun() {
    final DivisionInfo info = server.getInfo();

    final boolean run = isRunning && (info.isFollower() || info.isListener());
    if (!run) {
      LOG.info("{}: Stopping now (isRunning? {}, role = {})", this, isRunning, info.getCurrentRole());
    }
    return run;
  }

  @Override
  public  void run() {
    try {
      runImpl();
    } finally {
      stopped.complete(null);
    }
  }

  private boolean roleChangeChecking(TimeDuration electionTimeout) {
    /*
     * 当前没有待处理的操作（outstandingOp.get() == 0）。
     * 当前是 follower 角色（server.getInfo().isFollower()）。
     * 当前追随者的选举超时已到（lastRpcTime.elapsedTime().compareTo(electionTimeout) >= 0）。
     * 没有丧失大多数心跳（!lostMajorityHeartbeatsRecently()）。
     * Raft 服务器正在运行（server.isRunning()）。
     */
    return outstandingOp.get() == 0
            && isRunning && server.getInfo().isFollower()
            && lastRpcTime.elapsedTime().compareTo(electionTimeout) >= 0
            && !lostMajorityHeartbeatsRecently()
            && server.isRunning();
  }

  private void runImpl() {
    final TimeDuration sleepDeviationThreshold = server.getSleepDeviationThreshold();
    while (shouldRun()) {
      final TimeDuration electionTimeout = server.getRandomElectionTimeout();
      try {
        final TimeDuration extraSleep = electionTimeout.sleep();
        if (extraSleep.compareTo(sleepDeviationThreshold) > 0) {
          LOG.warn("Unexpected long sleep: sleep {} but took extra {} (> threshold = {})",
              electionTimeout, extraSleep, sleepDeviationThreshold);
          continue;
        }

        if (!shouldRun()) {
          break;
        }
        synchronized (server) {
          if (roleChangeChecking(electionTimeout)) {
            LOG.info("{}: change to CANDIDATE, lastRpcElapsedTime:{}, electionTimeout:{}",
                this, lastRpcTime.elapsedTime(), electionTimeout);
            server.getLeaderElectionMetrics().onLeaderElectionTimeout(); // Update timeout metric counters.
            // election timeout, should become a candidate
            server.changeToCandidate(false);
            break;
          }
        }
      } catch (InterruptedException e) {
        LOG.info("{} was interrupted", this);
        LOG.trace("TRACE", e);
        Thread.currentThread().interrupt();
        return;
      } catch (Exception e) {
        LOG.warn("{} caught an exception", this, e);
      }
    }
  }

  @Override
  public String toString() {
    return getName();
  }
}
