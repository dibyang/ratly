
package net.xdob.ratly.statemachine;

import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.proto.raft.RaftPeerRole;
import net.xdob.ratly.proto.raft.StateMachineLogEntryProto;
import net.xdob.ratly.protocol.RaftClientRequest;
import com.google.protobuf.ByteString;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.ReferenceCountedObject;
import net.xdob.ratly.util.ReflectionUtils;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * 事务的上下文，主要用于描述 Raft 协议中的事务处理过程。
 * TransactionContext 的主要作用是承载事务相关的数据，主要包括：
 *    事务发起的角色（如领导者或跟随者）。
 *    客户端请求（RaftClientRequest），当事务由客户端发起时。
 *    状态机日志条目，即 Raft 协议中的日志条目。
 *    事务是否已提交，以及在提交过程中是否出现异常。
 * <p>
 * 该事务可能源自客户端请求，或者可能来自通过 RAFT 日志传递的另一个状态机副本。
 * {@link TransactionContext} 可以通过 {@link StateMachine} 或状态机更新器创建。
 * <p>
 * 在第一种情况下，{@link StateMachine} 是领导者。
 * 当它收到 {@link StateMachine#startTransaction(RaftClientRequest)} 请求时，
 * 它会返回一个包含来自 {@link StateMachine} 更改的 {@link TransactionContext}。
 * 相同的上下文将通过 {@link StateMachine#applyTransaction(TransactionContext)}
 * 调用传回 {@link StateMachine}。
 * <p>
 * 在第二种情况下，{@link StateMachine} 是跟随者。
 * {@link TransactionContext} 将是来自领导者的 RAFT 日志中的已提交条目
 */
public interface TransactionContext {
  /**
   * 返回创建此事务上下文时的服务器角色，可能是 领导者（LEADER）或 跟随者（FOLLOWER）。
   * @return the role of the server when this context is created.
   */
  RaftPeerRole getServerRole();

  /**
   * 返回发起此事务的客户端请求{@link RaftClientRequest}。
   * @return the original request from the {@link RaftClientRequest}
   */
  RaftClientRequest getClientRequest();

  /**
   * 返回与状态机相关的日志条目，已被标记为废弃，推荐通过 getLogEntryRef() 或 getLogEntryUnsafe() 获取。
   * @return the data from the {@link StateMachine}
   * @deprecated access StateMachineLogEntry via {@link TransactionContext#getLogEntryRef()} or
   * {@link TransactionContext#getLogEntryUnsafe()}
   */
  @Deprecated
  StateMachineLogEntryProto getStateMachineLogEntry();

  /**
   * 如果事务执行失败，此方法将设置异常信息。
   */
  TransactionContext setException(Exception exception);

  /**
   * 获取与事务相关的异常，如果有的话。
   * @return the exception from the {@link StateMachine} or the log
   */
  Exception getException();

  /**
   * 设置与事务相关联的状态机上下文。状态机上下文指的是事务所依赖的状态机的内部状态。
   * @param stateMachineContext state machine context
   * @return transaction context specific to the given {@link StateMachine}
   */
  TransactionContext setStateMachineContext(Object stateMachineContext);

  /**
   * 获取与事务相关联的状态机上下文。状态机上下文指的是事务所依赖的状态机的内部状态。
   * @return the {@link StateMachine} the current {@link TransactionContext} specific to
   */
  Object getStateMachineContext();

  /**
   * 初始化一个新的日志条目，通常用于将事务写入 RAFT 日志。
   * Initialize {@link LogEntryProto} using the internal {@link StateMachineLogEntryProto}.
   * @param term The current term.
   * @param index The index of the log entry.
   * @return the result {@link LogEntryProto}
   */
  LogEntryProto initLogEntry(long term, long index);

  /**
   * 返回一个日志条目的副本
   * @return a copy of the committed log entry if it exists; otherwise, returns null
   *
   * @deprecated Use {@link #getLogEntryRef()} or {@link #getLogEntryUnsafe()} to avoid copying.
   */
  @Deprecated
  LogEntryProto getLogEntry();

  /**
   * @return the committed log entry if it exists; otherwise, returns null.
   *         The returned value is safe to use only before {@link StateMachine#applyTransaction} returns.
   *         Once {@link StateMachine#applyTransaction} has returned, it is unsafe to use the log entry
   *         since the underlying buffers can possiby be released.
   */
  default LogEntryProto getLogEntryUnsafe() {
    return getLogEntryRef().get();
  }

  /**
   * 返回一个引用计数的对象，可以安全地访问日志条目
   * Get a {@link ReferenceCountedObject} to the committed log entry.
   *
   * It is safe to access the log entry by calling {@link ReferenceCountedObject#get()}
   * (without {@link ReferenceCountedObject#retain()})
   * inside the scope of {@link StateMachine#applyTransaction}.
   *
   * If the log entry is needed after {@link StateMachine#applyTransaction} returns,
   * e.g. for asynchronous computation or caching,
   * the caller must invoke {@link ReferenceCountedObject#retain()} and {@link ReferenceCountedObject#release()}.
   *
   * @return a reference to the committed log entry if it exists; otherwise, returns null.
   */
  default ReferenceCountedObject<LogEntryProto> getLogEntryRef() {
    return Optional.ofNullable(getLogEntryUnsafe()).map(this::wrap).orElse(null);
  }

  /**
   * 用于包装日志条目。
   * Wrap the given log entry as a {@link ReferenceCountedObject} for retaining it for later use.
   */
  default ReferenceCountedObject<LogEntryProto> wrap(LogEntryProto entry) {
    Preconditions.assertSame(getLogEntry().getTerm(), entry.getTerm(), "entry.term");
    Preconditions.assertSame(getLogEntry().getIndex(), entry.getIndex(), "entry.index");
    return ReferenceCountedObject.wrap(entry);
  }

  /**
   * 设置事务应该被提交到 RAFT 日志中。
   * @param shouldCommit true if the transaction is supposed to be committed to the RAFT log
   * @return the current {@link TransactionContext} itself
   */
  TransactionContext setShouldCommit(boolean shouldCommit);

  /**
   * 事务是否应该被提交到 RAFT 日志中。
   * @return true if it commits the transaction to the RAFT log, otherwise, false
   */
  boolean shouldCommit();

  // proxy StateMachine methods. We do not want to expose the SM to the RaftLog

  /**
   * 在事务被附加到 RAFT 日志之前调用，通常用于执行一些准备工作；
   * <p>
   * This is called before the transaction passed from the StateMachine is appended to the raft log.
   * This method will be called from log append and having the same strict serial order that the
   * Transactions will have in the RAFT log. Since this is called serially in the critical path of
   * log append, it is important to do only required operations here.
   * @return The Transaction context.
   */
  TransactionContext preAppendTransaction() throws IOException;

  /**
   * 在事务无法附加时调用。
   * <p>
   * Called to notify the state machine that the Transaction passed cannot be appended (or synced).
   * The exception field will indicate whether there was an exception or not.
   * @return cancelled transaction
   */
  TransactionContext cancelTransaction() throws IOException;

  static Builder newBuilder() {
    return new Builder();
  }

  class Builder {
    private RaftPeerRole serverRole = RaftPeerRole.LEADER;
    private StateMachine stateMachine;
    private Object stateMachineContext;

    private RaftClientRequest clientRequest;
    private LogEntryProto logEntry;
    private StateMachineLogEntryProto stateMachineLogEntry;
    private ByteString logData;
    private ByteString stateMachineData;

    public Builder setServerRole(RaftPeerRole serverRole) {
      this.serverRole = serverRole;
      return this;
    }

    public Builder setStateMachine(StateMachine stateMachine) {
      this.stateMachine = stateMachine;
      return this;
    }

    public Builder setStateMachineContext(Object stateMachineContext) {
      this.stateMachineContext = stateMachineContext;
      return this;
    }

    public Builder setClientRequest(RaftClientRequest clientRequest) {
      this.clientRequest = clientRequest;
      return this;
    }

    public Builder setLogEntry(LogEntryProto logEntry) {
      this.logEntry = logEntry;
      return this;
    }

    public Builder setStateMachineLogEntry(StateMachineLogEntryProto stateMachineLogEntry) {
      this.stateMachineLogEntry = stateMachineLogEntry;
      return this;
    }

    public Builder setLogData(ByteString logData) {
      this.logData = logData;
      return this;
    }

    public Builder setStateMachineData(ByteString stateMachineData) {
      this.stateMachineData = stateMachineData;
      return this;
    }

    public TransactionContext build() {
      Objects.requireNonNull(serverRole, "serverRole == null");
      Objects.requireNonNull(stateMachine, "stateMachine == null");
      if (clientRequest != null) {
        Preconditions.assertTrue(serverRole == RaftPeerRole.LEADER,
            () -> "serverRole MUST be LEADER since clientRequest != null, serverRole is " + serverRole);
        Preconditions.assertNull(logEntry, () -> "logEntry MUST be null since clientRequest != null");
        return newTransactionContext(stateMachine, clientRequest,
            stateMachineLogEntry, logData, stateMachineData, stateMachineContext);
      } else {
        Objects.requireNonNull(logEntry, "logEntry must not be null since clientRequest == null");
        Preconditions.assertTrue(logEntry.hasStateMachineLogEntry(),
            () -> "Unexpected logEntry: stateMachineLogEntry not found, logEntry=" + logEntry);
        return newTransactionContext(serverRole, stateMachine, logEntry);
      }
    }

    /** Get the impl class using reflection. */
    private static final Class<? extends TransactionContext> IMPL_CLASS
        = ReflectionUtils.getImplClass(TransactionContext.class);

    /** @return a new {@link TransactionContext} using reflection. */
    private static TransactionContext newTransactionContext(
        StateMachine stateMachine, RaftClientRequest clientRequest,
        StateMachineLogEntryProto stateMachineLogEntry, ByteString logData, ByteString stateMachineData,
        Object stateMachineContext) {
      final Class<?>[] argClasses = {RaftClientRequest.class, StateMachine.class,
          StateMachineLogEntryProto.class, ByteString.class, ByteString.class, Object.class};
      return ReflectionUtils.newInstance(IMPL_CLASS, argClasses,
          clientRequest, stateMachine, stateMachineLogEntry, logData, stateMachineData, stateMachineContext);
    }

    /** @return a new {@link TransactionContext} using reflection. */
    private static TransactionContext newTransactionContext(
        RaftPeerRole serverRole, StateMachine stateMachine, LogEntryProto logEntry) {
      final Class<?>[] argClasses = {RaftPeerRole.class, StateMachine.class, LogEntryProto.class};
      return ReflectionUtils.newInstance(IMPL_CLASS, argClasses, serverRole, stateMachine, logEntry);
    }
  }
}
