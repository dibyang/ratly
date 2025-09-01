package net.xdob.ratly.statemachine.impl;

import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftClientRequest;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.storage.RaftStorage;
import net.xdob.ratly.statemachine.*;
import com.google.protobuf.InvalidProtocolBufferException;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.LifeCycle;
import net.xdob.ratly.util.MemoizedSupplier;
import net.xdob.ratly.util.Preconditions;

import java.io.IOException;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 一个基础实现，用于实现 Raft 状态机的核心功能，集成了状态管理、事务应用、查询等功能。
 * 该类实现了多个接口，包括 StateMachine、DataApi、EventApi、LeaderEventApi
 * 和 FollowerEventApi，这些接口定义了状态机与 Raft 协议及其他组件交互的方法。
 */
public class BaseStateMachine implements StateMachine, DataApi,
    EventApi, LeaderEventApi, FollowerEventApi {
  /**
   * 一个 CompletableFuture<RaftServer> 对象，表示当前状态机的 Raft 服务器实例。
   * 当状态机初始化时，服务器会被设置到 server 字段中。
   */
  private final CompletableFuture<RaftServer> server = new CompletableFuture<>();

  /**
   * Raft 集群的 ID，表示当前状态机所在的 Raft 集群。
   */
  private volatile RaftGroupId groupId;

  private volatile RaftPeerId peerId;

  /**
   * 状态机的生命周期管理器，跟踪状态机的生命周期状态。
   */
  private final LifeCycle lifeCycle = new LifeCycle(JavaUtils.getClassSimpleName(getClass()));
  /**
   * 保存最近应用的日志条目的 TermIndex，它是一个原子引用，确保在多线程环境中安全操作。
   */
  private final AtomicReference<TermIndex> lastAppliedTermIndex = new AtomicReference<>();
  /**
   * 一个有序的 TreeMap，用于存储待完成的事务 CompletableFuture 对象，按照日志索引排序。
   */
  private final SortedMap<Long, CompletableFuture<Void>> transactionFutures = new TreeMap<>();

  /**
   * 构造函数初始化 lastAppliedTermIndex 为 TermIndex.INITIAL_VALUE，确保状态机从初始状态开始。
   */
  public BaseStateMachine() {
    setLastAppliedTermIndex(TermIndex.INITIAL_VALUE);
  }

  public RaftPeerId getId() {
    return server.isDone()? server.join().getId(): null;
  }

  public LifeCycle getLifeCycle() {
    return lifeCycle;
  }

  public CompletableFuture<RaftServer> getServer() {
    return server;
  }

  public RaftGroupId getGroupId() {
    return groupId;
  }

  @Override
  public LifeCycle.State getLifeCycleState() {
    return lifeCycle.getCurrentState();
  }

  @Override
  public void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftPeerId peerId, RaftStorage storage, MemoizedSupplier<ServerStateSupport> logQuery) throws IOException {
    this.groupId = raftGroupId;
    this.peerId = peerId;
    this.server.complete(raftServer);
    lifeCycle.setName("" + this);
  }

  @Override
  public SnapshotInfo getLatestSnapshot() {
    return getStateMachineStorage().getLatestSnapshot();
  }

	@Override
  public void pause() {

  }

  @Override
  public void reinitialize() throws IOException {
  }

  @Override
  public TransactionContext applyTransactionSerial(TransactionContext trx) throws InvalidProtocolBufferException {
    return trx;
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    // return the same message contained in the entry
    final LogEntryProto entry = Objects.requireNonNull(trx.getLogEntryUnsafe());
    updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
    return CompletableFuture.completedFuture(
        Message.valueOf(entry.getStateMachineLogEntry().getLogData()));
  }

  @Override
  public TermIndex getLastAppliedTermIndex() {
    return lastAppliedTermIndex.get();
  }

  protected void setLastAppliedTermIndex(TermIndex newTI) {
    lastAppliedTermIndex.set(newTI);
  }

  @Override
  public void notifyTermIndexUpdated(long term, long index) {
    updateLastAppliedTermIndex(term, index);
  }

  protected boolean updateLastAppliedTermIndex(long term, long index) {
    return updateLastAppliedTermIndex(TermIndex.valueOf(term, index));
  }

  protected boolean updateLastAppliedTermIndex(TermIndex newTI) {
    Objects.requireNonNull(newTI, "newTI == null");
    final TermIndex oldTI = lastAppliedTermIndex.getAndSet(newTI);
    if (!newTI.equals(oldTI)) {
      LOG.trace("{}: update lastAppliedTermIndex from {} to {}", getId(), oldTI, newTI);
      if (oldTI != null) {
        Preconditions.assertTrue(newTI.compareTo(oldTI) >= 0,
            () -> getId() + ": Failed updateLastAppliedTermIndex: newTI = "
                + newTI + " < oldTI = " + oldTI);
      }
      return true;
    }

    synchronized (transactionFutures) {
      for(long i; !transactionFutures.isEmpty() && (i = transactionFutures.firstKey()) <= newTI.getIndex(); ) {
        transactionFutures.remove(i).complete(null);
      }
    }
    return false;
  }

  @Override
  public long takeSnapshot() throws IOException {
    return RaftLog.INVALID_LOG_INDEX;
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return new StateMachineStorage() {
      @Override
      public void init(RaftStorage raftStorage) throws IOException {
      }

      @Override
      public SnapshotInfo getLatestSnapshot() {
        return null;
      }

      @Override
      public void format() throws IOException {
      }

      @Override
      public void cleanupOldSnapshots(SnapshotRetentionPolicy snapshotRetentionPolicy) {
      }
    };
  }

  @Override
  public CompletableFuture<Message> queryStale(Message request, long minIndex) {
    if (getLastAppliedTermIndex().getIndex() < minIndex) {
      synchronized (transactionFutures) {
        if (getLastAppliedTermIndex().getIndex() < minIndex) {
          return transactionFutures.computeIfAbsent(minIndex, key -> new CompletableFuture<>())
              .thenCompose(v -> query(request));
        }
      }
    }
    return query(request);
  }

	@Override
	public CompletableFuture<Message> heart(Message request) {
		return CompletableFuture.completedFuture(null);
	}

  @Override
  public CompletableFuture<Message> query(Message request) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
    return TransactionContext.newBuilder()
        .setStateMachine(this)
        .setClientRequest(request)
        .build();
  }

  @Override
  public TransactionContext cancelTransaction(TransactionContext trx) throws IOException {
    return trx;
  }

  @Override
  public TransactionContext preAppendTransaction(TransactionContext trx) throws IOException {
    return trx;
  }

  @Override
  public void close() throws IOException {
    // do nothing
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass()) + ":"
        + (!server.isDone()? "uninitialized": getId() + ":" + groupId);
  }
}
