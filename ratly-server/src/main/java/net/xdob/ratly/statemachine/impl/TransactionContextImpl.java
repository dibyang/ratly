package net.xdob.ratly.statemachine.impl;

import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.proto.raft.RaftPeerRole;
import net.xdob.ratly.proto.raft.StateMachineLogEntryProto;
import net.xdob.ratly.protocol.RaftClientRequest;
import net.xdob.ratly.server.raftlog.LogProtoUtils;
import net.xdob.ratly.statemachine.StateMachine;
import net.xdob.ratly.statemachine.TransactionContext;
import com.google.protobuf.ByteString;
import net.xdob.ratly.util.MemoizedSupplier;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.ReferenceCountedObject;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * 是 {@link TransactionContext} 接口的一个实现类，
 * 用于表示一个与 Raft 协议相关的事务上下文。
 * 它封装了与事务相关的各种信息，包括请求、状态机日志条目、异常、日志条目等，并提供了处理事务的各种操作。
 */
public class TransactionContextImpl implements TransactionContext {
  /**
   * 创建此对象时服务器的角色（如 LEADER、FOLLOWER）。
   */
  private final RaftPeerRole serverRole;
  /**
   * 关联的状态机对象{@link StateMachine}。
   * 这个状态机发起了事务请求。
   */
  private final StateMachine stateMachine;

  /**
   * 客户端的请求对象，它发起了事务。
   */
  private final RaftClientRequest clientRequest;

  /**
   * 可能在状态机{@link StateMachine}处理过程中抛出的异常，用于在事务中进行错误处理。
   */
  private volatile Exception exception;

  /**
   * 状态机{@link StateMachine}日志条目，包含了关于事务的数据。
   */
  private final StateMachineLogEntryProto stateMachineLogEntry;

  /**
   * 用于携带状态机在事务过程中的额外上下文，通常是状态机内部的自定义数据。
   * The {@link StateMachine} can use this object to carry state between
   * {@link StateMachine#startTransaction(RaftClientRequest)} and
   * {@link StateMachine#applyTransaction(TransactionContext)}.
   */
  private volatile Object stateMachineContext;

  /**
   * 表示是否应该将事务提交到 Raft 日志。
   * 如果为 false，则该事务将不会提交。
   */
  private boolean shouldCommit = true;

  /**
   * 已提交的日志条目，表示这个事务已经成功写入 Raft 日志。
   */
  private volatile LogEntryProto logEntry;
  /**
   * 日志条目的副本，用于延迟计算和缓存条目的数据。
   */
  private volatile Supplier<LogEntryProto> logEntryCopy;

  /**
   * 用于包装 {@link #logEntry}  的引用计数对象，以便在事务结束时释放底层缓冲区。
   */
  private volatile ReferenceCountedObject<?> delegatedRef;

  /**
   * 记录日志条目的索引，通过 CompletableFuture 异步计算。
   */
  private final CompletableFuture<Long> logIndexFuture = new CompletableFuture<>();

  private TransactionContextImpl(RaftPeerRole serverRole, RaftClientRequest clientRequest, StateMachine stateMachine,
      StateMachineLogEntryProto stateMachineLogEntry) {
    this.serverRole = serverRole;
    this.clientRequest = clientRequest;
    this.stateMachine = stateMachine;
    this.stateMachineLogEntry = stateMachineLogEntry;
  }

  /**
   * Construct a {@link TransactionContext} from a client request.
   * Used by the state machine to start a transaction
   * and send the Log entry representing the transaction data
   * to be applied to the raft log.
   */
  TransactionContextImpl(RaftClientRequest clientRequest, StateMachine stateMachine,
      StateMachineLogEntryProto stateMachineLogEntry, ByteString logData, ByteString stateMachineData,
      Object stateMachineContext) {
    this(RaftPeerRole.LEADER, clientRequest, stateMachine,
        get(stateMachineLogEntry, clientRequest, logData, stateMachineData));
    this.stateMachineContext = stateMachineContext;
  }

  private static StateMachineLogEntryProto get(StateMachineLogEntryProto stateMachineLogEntry,
      RaftClientRequest clientRequest, ByteString logData, ByteString stateMachineData) {
    if (stateMachineLogEntry != null) {
      return stateMachineLogEntry;
    } else {
      return LogProtoUtils.toStateMachineLogEntryProto(clientRequest, logData, stateMachineData);
    }
  }

  /**
   * Construct a {@link TransactionContext} from a {@link LogEntryProto}.
   * Used by followers for applying committed entries to the state machine.
   * @param logEntry the log entry to be applied
   */
  TransactionContextImpl(RaftPeerRole serverRole, StateMachine stateMachine, LogEntryProto logEntry) {
    this(serverRole, null, stateMachine, logEntry.getStateMachineLogEntry());
    setLogEntry(logEntry);
    this.logIndexFuture.complete(logEntry.getIndex());
  }

  @Override
  public RaftPeerRole getServerRole() {
    return serverRole;
  }

  @Override
  public RaftClientRequest getClientRequest() {
    return clientRequest;
  }

  public void setDelegatedRef(ReferenceCountedObject<?> ref) {
    this.delegatedRef = ref;
  }

  /**
   * 用于包装日志条目。
   */
  @Override
  public ReferenceCountedObject<LogEntryProto> wrap(LogEntryProto entry) {
    if (delegatedRef == null) {
      return TransactionContext.super.wrap(entry);
    }
    final LogEntryProto expected = getLogEntryUnsafe();
    Objects.requireNonNull(expected, "logEntry == null");
    Preconditions.assertSame(expected.getTerm(), entry.getTerm(), "entry.term");
    Preconditions.assertSame(expected.getIndex(), entry.getIndex(), "entry.index");
    return delegatedRef.delegate(entry);
  }

  @Override
  public StateMachineLogEntryProto getStateMachineLogEntry() {
    return stateMachineLogEntry;
  }

  @Override
  public Exception getException() {
    return exception;
  }

  @Override
  public TransactionContext setStateMachineContext(Object context) {
    this.stateMachineContext = context;
    return this;
  }

  @Override
  public Object getStateMachineContext() {
    return stateMachineContext;
  }

  @Override
  public LogEntryProto initLogEntry(long term, long index) {
    Preconditions.assertTrue(serverRole == RaftPeerRole.LEADER);
    Preconditions.assertNull(logEntry, "logEntry");
    Objects.requireNonNull(stateMachineLogEntry, "stateMachineLogEntry == null");

    logIndexFuture.complete(index);
    return setLogEntry(LogProtoUtils.toLogEntryProto(stateMachineLogEntry, term, index));
  }

  public CompletableFuture<Long> getLogIndexFuture() {
    return logIndexFuture;
  }

  private LogEntryProto setLogEntry(LogEntryProto entry) {
    this.logEntry = entry;
    this.logEntryCopy = MemoizedSupplier.valueOf(() -> LogProtoUtils.copy(entry));
    return entry;
  }


  @Override
  public LogEntryProto getLogEntry() {
    return logEntryCopy == null ? null : logEntryCopy.get();
  }

  @Override
  public LogEntryProto getLogEntryUnsafe() {
    return logEntry;
  }


  @Override
  public TransactionContext setException(Exception ioe) {
    this.exception = ioe;
    return this;
  }

  @Override
  public TransactionContext setShouldCommit(boolean sCommit) {
    this.shouldCommit = sCommit;
    return this;
  }

  @Override
  public boolean shouldCommit() {
    // TODO: Hook this up in the server to bypass the RAFT Log and send back a response to client
    return this.shouldCommit;
  }

  @Override
  public TransactionContext preAppendTransaction() throws IOException {
    return stateMachine.preAppendTransaction(this);
  }

  @Override
  public TransactionContext cancelTransaction() throws IOException {
    // TODO: This is not called from Raft server / log yet. When an IOException happens, we should
    // call this to let the SM know that Transaction cannot be synced
    return stateMachine.cancelTransaction(this);
  }

  public static LogEntryProto getLogEntry(TransactionContext context) {
    return ((TransactionContextImpl) context).logEntry;
  }
}
