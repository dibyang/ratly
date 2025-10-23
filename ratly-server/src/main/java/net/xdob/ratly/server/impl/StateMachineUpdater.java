package net.xdob.ratly.server.impl;

import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.metrics.Timekeeper;
import net.xdob.ratly.proto.raft.CommitInfoProto;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.exceptions.StateMachineException;
import net.xdob.ratly.server.config.RaftServerConfigKeys;
import net.xdob.ratly.server.exception.MustStopNode;
import net.xdob.ratly.server.exception.SnapshotException;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.LogProtoUtils;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.server.raftlog.RaftLogIOException;
import net.xdob.ratly.server.raftlog.RaftLogIndex;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.statemachine.StateMachine;
import net.xdob.ratly.statemachine.SnapshotRetentionPolicy;
import net.xdob.ratly.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.LongStream;

/**
 * 监控提交索引，将日志提交到状态机执行，并定期生成快照以减少日志量。
 * <p>
 * 该类用于跟踪在仲裁中已提交的日志条目，并将它们应用到状态机中。
 * 我们让一个单独的线程异步地完成这项工作，以确保这不会阻塞正常的Raft协议操作。
 * 如果启用了自动日志压缩功能，当日志大小超过预设的限制时，
 * 状态机更新线程会通过调用 {@link StateMachine#takeSnapshot} 方法来触发状态机的快照操作。
 */
class StateMachineUpdater implements Runnable {
  static final Logger LOG = LoggerFactory.getLogger(StateMachineUpdater.class);

  enum State {
    RUNNING, STOP, RELOAD, EXCEPTION
  }

  private final Consumer<Object> infoIndexChange;
  private final Consumer<Object> debugIndexChange;
  private final String name;

  private final StateMachine stateMachine;
  private final RaftServerImpl server;
  private final RaftLog raftLog;

  private final boolean triggerSnapshotWhenStopEnabled;

  private final boolean triggerSnapshotWhenRemoveEnabled;

  private final Long autoSnapshotThreshold;
  private final boolean purgeUptoSnapshotIndex;

  private final Thread updater;
  private final AwaitForSignal awaitForSignal;

  private final RaftLogIndex appliedIndex;
  private final RaftLogIndex snapshotIndex;
  private final AtomicReference<Long> stopIndex = new AtomicReference<>();
  private volatile State state = State.RUNNING;

  private final SnapshotRetentionPolicy snapshotRetentionPolicy;

  private final MemoizedSupplier<StateMachineMetrics> stateMachineMetrics;

  private final Consumer<Long> appliedIndexConsumer;

  private volatile boolean isRemoving;

  StateMachineUpdater(StateMachine stateMachine, RaftServerImpl server,
      ServerState serverState, long lastAppliedIndex, RaftProperties properties, Consumer<Long> appliedIndexConsumer) {
    this.name = serverState.getMemberId() + "-" + JavaUtils.getClassSimpleName(getClass());
    this.appliedIndexConsumer = appliedIndexConsumer;
    this.infoIndexChange = s -> LOG.info("{}: {}", name, s);
    this.debugIndexChange = s -> LOG.debug("{}: {}", name, s);

    this.stateMachine = stateMachine;
    this.server = server;
    this.raftLog = serverState.getLog();

    this.appliedIndex = new RaftLogIndex("appliedIndex", lastAppliedIndex);
    this.snapshotIndex = new RaftLogIndex("snapshotIndex", lastAppliedIndex);

    this.triggerSnapshotWhenStopEnabled = RaftServerConfigKeys.Snapshot.triggerWhenStopEnabled(properties);
    this.triggerSnapshotWhenRemoveEnabled = RaftServerConfigKeys.Snapshot.triggerWhenRemoveEnabled(properties);
    final boolean autoSnapshot = RaftServerConfigKeys.Snapshot.autoTriggerEnabled(properties);
    this.autoSnapshotThreshold = autoSnapshot? RaftServerConfigKeys.Snapshot.autoTriggerThreshold(properties): null;
    final int numSnapshotFilesRetained = RaftServerConfigKeys.Snapshot.retentionFileNum(properties);
    this.snapshotRetentionPolicy = new SnapshotRetentionPolicy() {
      @Override
      public int getNumSnapshotsRetained() {
        return numSnapshotFilesRetained;
      }
    };
    this.purgeUptoSnapshotIndex = RaftServerConfigKeys.Log.purgeUptoSnapshotIndex(properties);
    updater = Daemon.newBuilder().setName(name).setRunnable(this)
        .setThreadGroup(server.getThreadGroup()).build();
    this.awaitForSignal = new AwaitForSignal(name);
    this.stateMachineMetrics = MemoizedSupplier.valueOf(
        () -> StateMachineMetrics.getStateMachineMetrics(server, appliedIndex, stateMachine));
  }

  void start() {
    //wait for RaftServerImpl and ServerState constructors to complete
    stateMachineMetrics.get();
    updater.start();
    notifyAppliedIndex(appliedIndex.get());
  }

  private void stop() {
    state = State.STOP;
    try {
      LOG.info("{}: closing {}, lastApplied={}", name,
          JavaUtils.getClassSimpleName(stateMachine.getClass()), stateMachine.getLastAppliedTermIndex());
      stateMachine.close();
      if (stateMachineMetrics.isInitialized()) {
        stateMachineMetrics.get().unregister();
      }
    } catch(Throwable t) {
      LOG.warn(name + ": Failed to close " + JavaUtils.getClassSimpleName(stateMachine.getClass())
          + " " + stateMachine, t);
    }
  }

  /**
   * Stop the updater thread after all the committed transactions
   * have been applied to the state machine.
   */
  void stopAndJoin() throws InterruptedException {
    if (state == State.EXCEPTION) {
      stop();
      return;
    }
    if (stopIndex.compareAndSet(null, raftLog.getLastCommittedIndex())) {
      notifyUpdater();
      LOG.info("{}: set stopIndex = {}", this, stopIndex);
    }
    updater.join();
  }

  void reloadStateMachine() {
    state = State.RELOAD;
    notifyUpdater();
  }

  void notifyUpdater() {
    awaitForSignal.signal();
  }

  @Override
  public String toString() {
    return name;
  }

  /**
   * 该方法用于持续更新状态机，直到状态变为 STOP。其主要功能如下：
   *   1. 等待提交：调用 waitForCommit() 等待日志被提交。
   *   2. 重载处理：如果状态是 RELOAD，则重新加载状态机。
   *   3. 应用日志：调用 applyLog() 将已提交的日志条目应用到状态机。
   *   4. 检查并拍快照：根据条件决定是否拍摄快照。
   *   5. 停止判断：若满足停止条件，则执行停止操作。
   *   6. 异常处理：捕获所有异常，记录错误，并触发服务器停止。
   */
  @Override
  public void run() {
    for(; state != State.STOP; ) {
      try {
        waitForCommit();

        if (state == State.RELOAD) {
          reload();
        }

        final MemoizedSupplier<List<CompletableFuture<Message>>> futures = applyLog();
        checkAndTakeSnapshot(futures);

        if (shouldStop()) {
          checkAndTakeSnapshot(futures);
          stop();
        }
      } catch (Throwable t) {
        if (t instanceof InterruptedException && state == State.STOP) {
          Thread.currentThread().interrupt();
          LOG.info("{} was interrupted.  Exiting ...", this);
        } else {
          state = State.EXCEPTION;
          LOG.error(this + " caught a Throwable.", t);
					if(t instanceof MustStopNode) {
						server.stopSeverState();
						//server.close();
					}
        }
      }
    }
  }

  private void waitForCommit() throws InterruptedException {
    // When a peer starts, the committed is initialized to 0.
    // It will be updated only after the leader contacts other peers.
    // Thus it is possible to have applied > committed initially.
    final long applied = getLastAppliedIndex();
    for(; applied >= raftLog.getLastCommittedIndex() && state == State.RUNNING && !shouldStop(); ) {
      if (awaitForSignal.await(100, TimeUnit.MILLISECONDS)) {
        return;
      }
    }
  }

  private void reload() throws IOException {
    //Preconditions.assertTrue(state == State.RELOAD);

    stateMachine.reinitialize();

    final SnapshotInfo snapshot = stateMachine.getLatestSnapshot();
    Objects.requireNonNull(snapshot, "snapshot == null");
    final long i = snapshot.getIndex();
    snapshotIndex.setUnconditionally(i, infoIndexChange);
    appliedIndex.setUnconditionally(i, infoIndexChange);
    notifyAppliedIndex(i);
    state = State.RUNNING;
  }

  /**
   * 将日志中已提交但尚未应用到状态机的条目逐个应用到状态机中，并记录应用进度。
   * 具体逻辑如下：
   *   1. 获取当前已提交的日志索引 committed。
   *   2. 循环处理从 appliedIndex + 1 开始直到没有新日志可应用或状态不是 RUNNING 或需要停止。
   *   3. 对每个待应用的日志条目调用 raftLog.retainLog(nextIndex) 获取日志对象。
   *   4. 若日志为 null，说明可能需要加载快照，跳出循环。
   *   5. 使用 server.applyLogToStateMachine(next) 将日志条目异步应用到状态机。
   *   6. 更新 appliedIndex 并确保其正确递增。
   *   7. 如果有返回的 CompletableFuture<Message>，则加入 futures 列表并注册回调通知应用完成；否则直接通知完成。
   *   8. 每次处理完一个日志条目后调用 next.release() 释放资源。
   *   9. 最终返回包含所有未完成 Future 的 MemoizedSupplier。
   */
  private MemoizedSupplier<List<CompletableFuture<Message>>> applyLog() throws RaftLogIOException {
    final MemoizedSupplier<List<CompletableFuture<Message>>> futures = MemoizedSupplier.valueOf(ArrayList::new);
    final long committed = raftLog.getLastCommittedIndex();
    for(long applied; (applied = getLastAppliedIndex()) < committed && state == State.RUNNING && !shouldStop(); ) {
      final long nextIndex = applied + 1;
      final ReferenceCountedObject<LogEntryProto> next = raftLog.retainLog(nextIndex);
      if (next == null) {
        LOG.debug("{}: logEntry {} is null. There may be snapshot to load. state:{}",
            this, nextIndex, state);
        break;
      }

      try {
        final LogEntryProto entry = next.get();
        if (LOG.isTraceEnabled()) {
          LOG.trace("{}: applying nextIndex={}, nextLog={}", this, nextIndex, LogProtoUtils.toLogEntryString(entry));
        } else {
          LOG.debug("{}: applying nextIndex={}", this, nextIndex);
        }

        final CompletableFuture<Message> f = server.applyLogToStateMachine(next);
        final long incremented = appliedIndex.incrementAndGet(debugIndexChange);
        Preconditions.assertTrue(incremented == nextIndex);
        if (f != null) {
          futures.get().add(f);
          f.thenAccept(m -> notifyAppliedIndex(incremented));
        } else {
          notifyAppliedIndex(incremented);
        }
      } finally {
        next.release();
      }
    }
    return futures;
  }

  private void checkAndTakeSnapshot(MemoizedSupplier<List<CompletableFuture<Message>>> futures)
      throws ExecutionException, InterruptedException {
    // check if need to trigger a snapshot
    if (shouldTakeSnapshot()) {
      if (futures.isInitialized()) {
        JavaUtils.allOf(futures.get()).get();
      }

      takeSnapshot();
    }
  }

  private void takeSnapshot() {
    final long i;
    try {
      try(UncheckedAutoCloseable ignored = Timekeeper.start(stateMachineMetrics.get().getTakeSnapshotTimer())) {
        i = stateMachine.takeSnapshot();
      }
      server.getSnapshotRequestHandler().completeTakingSnapshot(i);

      final long lastAppliedIndex = getLastAppliedIndex();
      if (i > lastAppliedIndex) {
        throw new StateMachineException(
            "Bug in StateMachine: snapshot index = " + i + " > appliedIndex = " + lastAppliedIndex
            + "; StateMachine class=" +  stateMachine.getClass().getName() + ", stateMachine=" + stateMachine);
      }
      stateMachine.getStateMachineStorage().cleanupOldSnapshots(snapshotRetentionPolicy);
    } catch (IOException e) {
      //LOG.error(name + ": Failed to take snapshot", e);
			throw new SnapshotException(name + ": Failed to take snapshot", e);
    }

    if (i >= 0) {
      LOG.info("{}: Took a snapshot at index {}", name, i);
      snapshotIndex.updateIncreasingly(i, infoIndexChange);

      final long purgeIndex;
      if (purgeUptoSnapshotIndex) {
        // We can purge up to snapshot index even if all the peers do not have
        // commitIndex up to this snapshot index.
        purgeIndex = i;
      } else {
        final LongStream commitIndexStream = server.getCommitInfos().stream().mapToLong(
            CommitInfoProto::getCommitIndex);
        purgeIndex = LongStream.concat(LongStream.of(i), commitIndexStream).min().orElse(i);
      }
      raftLog.purge(purgeIndex);
    }
  }

  private boolean shouldStop() {
    return Optional.ofNullable(stopIndex.get()).filter(i -> i <= getLastAppliedIndex()).isPresent();
  }

  private boolean shouldTakeSnapshot() {
    if (state == State.RUNNING && server.getSnapshotRequestHandler().shouldTriggerTakingSnapshot()) {
      return true;
    }
    if (autoSnapshotThreshold == null) {
      return false;
    } else if (shouldStop()) {
      return shouldTakeSnapshotAtStop() && getLastAppliedIndex() - snapshotIndex.get() > 0;
    }

//    return state == State.RUNNING &&
//        getStateMachineLastAppliedIndex() - snapshotIndex.get() >= autoSnapshotThreshold;
    return state == State.RUNNING &&
        stateMachine.getLastTxAppliedTermIndex().getIndex() - snapshotIndex.get() >= autoSnapshotThreshold;
  }

  /**
   * In view of the three variables triggerSnapshotWhenStopEnabled, triggerSnapshotWhenRemoveEnabled and isRemoving,
   * we can draw the following 8 combination:
   * true true true => true
   * true true false => true
   * true false true => false
   * true false false => true
   * false true true => true
   * false true false =>  false
   * false false true => false
   * false false false => false
   * @return result
   */
  private boolean shouldTakeSnapshotAtStop() {
    return isRemoving ? triggerSnapshotWhenRemoveEnabled : triggerSnapshotWhenStopEnabled;
  }

  void setRemoving() {
    this.isRemoving = true;
  }

  public long getLastAppliedIndex() {
    return appliedIndex.get();
  }

  private void notifyAppliedIndex(long index) {
    appliedIndexConsumer.accept(index);
  }

  long getStateMachineLastAppliedIndex() {
    return Optional.ofNullable(stateMachine.getLastAppliedTermIndex())
        .map(TermIndex::getIndex)
        .orElse(RaftLog.INVALID_LOG_INDEX);
  }
}
