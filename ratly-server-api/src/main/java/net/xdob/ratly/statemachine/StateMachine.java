package net.xdob.ratly.statemachine;

import net.xdob.ratly.proto.raft.*;
import net.xdob.ratly.protocol.ClientInvocationId;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftClientRequest;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.config.Snapshot;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.storage.RaftStorage;
import com.google.protobuf.InvalidProtocolBufferException;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Raft 协议中用于处理日志条目和更新的核心组件，代表了应用状态的封装和管理。
 * 它是用户自定义的状态机实现的入口，所有的状态机操作都可以通过这个接口定义和执行。
 * Raft 协议的主要思想是将集群中的一致性操作（例如日志条目的复制）交给状态机去应用，从而实现数据的一致性和容错。
 * (see <a href="https://en.wikipedia.org/wiki/State_machine_replication">https://en.wikipedia.org/wiki/State_machine_replication</a>).
 * <p>
 *  A {@link StateMachine} implementation must be threadsafe.
 *  For example, the {@link #applyTransaction(TransactionContext)} method and the {@link #query(Message)} method
 *  can be invoked in parallel.
 */
public interface StateMachine extends Closeable {
  Logger LOG = LoggerFactory.getLogger(StateMachine.class);

  /** A registry to support different state machines in multi-raft environment. */
  interface Registry extends Function<RaftGroupId, StateMachine> {
  }

  /**
   * 功能：返回与状态机相关的 {@link DataApi} 对象，用于处理 Raft 日志外的数据管理。
   * 说明：如果状态机支持 DataApi，该方法会返回状态机实例自身，否则会返回 DataApi.DEFAULT，即默认的无操作实现。
   * <p>
   * If this {@link StateMachine} chooses to support the optional {@link DataApi},
   * it may either implement {@link DataApi} directly or override this method to return a {@link DataApi} object.
   * Otherwise, this {@link StateMachine} does not support {@link DataApi}.
   * Then, this method returns the default noop {@link DataApi} object.
   *
   * @return The {@link DataApi} object.
   */
  default DataApi data() {
    return this instanceof DataApi? (DataApi)this : DataApi.DEFAULT;
  }

  /**
   * 功能：返回与状态机相关的 {@link EventApi} 对象，用于处理事件的管理。
   * 说明：与 data() 方法类似，如果状态机支持 EventApi，会返回其实现，否则返回 EventApi.DEFAULT。
   * <p>
   * If this {@link StateMachine} chooses to support the optional {@link EventApi},
   * it may either implement {@link EventApi} directly or override this method to return an {@link EventApi} object.
   * Otherwise, this {@link StateMachine} does not support {@link EventApi}.
   * Then, this method returns the default noop {@link EventApi} object.
   *
   * @return The {@link EventApi} object.
   */
  default EventApi event() {
    return this instanceof EventApi ? (EventApi)this : EventApi.DEFAULT;
  }

  /**
   * 功能：返回与领导者事件相关的 {@link LeaderEventApi} 对象，允许状态机实现领导者相关的事件处理。
   * 说明：如果状态机支持 {@link LeaderEventApi}，返回状态机实例；否则返回默认的 LeaderEventApi.DEFAULT。
   * <p>
   * If this {@link StateMachine} chooses to support the optional {@link LeaderEventApi},
   * it may either implement {@link LeaderEventApi} directly
   * or override this method to return an {@link LeaderEventApi} object.
   * Otherwise, this {@link StateMachine} does not support {@link LeaderEventApi}.
   * Then, this method returns the default noop {@link LeaderEventApi} object.
   *
   * @return The {@link LeaderEventApi} object.
   */
  default LeaderEventApi leaderEvent() {
    return this instanceof LeaderEventApi? (LeaderEventApi)this : LeaderEventApi.DEFAULT;
  }

  /**
   * 功能：返回与跟随者事件相关的 {@link FollowerEventApi} 对象，允许状态机处理跟随者相关的事件。
   * 说明：如果状态机支持 {@link FollowerEventApi}，返回状态机实例；否则返回默认的 FollowerEventApi.DEFAULT。
   * <p>
   * If this {@link StateMachine} chooses to support the optional {@link FollowerEventApi},
   * it may either implement {@link FollowerEventApi} directly
   * or override this method to return an {@link FollowerEventApi} object.
   * Otherwise, this {@link StateMachine} does not support {@link FollowerEventApi}.
   * Then, this method returns the default noop {@link FollowerEventApi} object.
   *
   * @return The {@link LeaderEventApi} object.
   */
  default FollowerEventApi followerEvent() {
    return this instanceof FollowerEventApi? (FollowerEventApi)this : FollowerEventApi.DEFAULT;
  }

  /**
   * 功能：初始化状态机，加载最新的快照并进行必要的设置。
   * 说明：该方法必须读取存储中的最新快照（如果存在）并进行状态机的初始化。
   */
  void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage storage) throws IOException;

  /**
   * 功能：返回状态机的生命周期状态。
   * 说明：用于了解状态机当前的生命周期状态，例如是否处于初始化、运行或暂停状态。
   * @return the lifecycle state.
   */
  LifeCycle.State getLifeCycleState();

  /**
   * 功能：暂停状态机，关闭所有打开的文件以便可以安装新的快照。
   * 说明：该方法可以在快照安装之前执行，确保当前的状态机数据被安全地保存。
   */
  void pause();

  /**
   * 功能：在暂停状态下重新初始化状态机，读取文件系统中的最新快照并进行初始化。
   * 说明：如果状态机被暂停，这个方法会恢复它的状态。
   */
  void reinitialize() throws IOException;

  /**
   * 功能：将内存中的状态快照到 Raft 存储中，返回已应用的日志条目的最大索引。
   * 说明：状态机可以决定何时、如何以及是否阻塞来进行快照。快照应该包含最新的 Raft 配置。
   * <p>
   * Dump the in-memory state into a snapshot file in the RaftStorage. The
   * StateMachine implementation can decide 1) its own snapshot format, 2) when
   * a snapshot is taken, and 3) how the snapshot is taken (e.g., whether the
   * snapshot blocks the state machine, and whether to purge log entries after
   * a snapshot is done).
   *
   * In the meanwhile, when the size of raft log outside of the latest snapshot
   * exceeds certain threshold, the RaftServer may choose to trigger a snapshot
   * if {@link Snapshot#AUTO_TRIGGER_ENABLED_KEY} is enabled.
   *
   * The snapshot should include the latest raft configuration.
   *
   * @return the largest index of the log entry that has been applied to the
   *         state machine and also included in the snapshot. Note the log purge
   *         should be handled separately.
   */
  // TODO: refactor this
  long takeSnapshot() throws IOException;

  /**
   * 功能：返回与状态机相关的持久化存储对象。
   * 说明：用于与状态机的持久化存储进行交互。
   * @return StateMachineStorage to interact with the durability guarantees provided by the
   * state machine.
   */
  StateMachineStorage getStateMachineStorage();

  /**
   * 功能：返回最新的持久化快照信息。
   * 说明：通过此方法可以查询到当前状态机的最新快照信息。
   */
  SnapshotInfo getLatestSnapshot();

  /**
   * 功能：查询状态机，查询请求必须是只读操作。
   * 说明：该方法允许外部组件（例如客户端）通过查询访问状态机的只读数据。
   */
  CompletableFuture<Message> query(Message request);

  /**
   * 功能：查询状态机，但请求的 minIndex 必须小于等于当前提交索引，可能返回过时数据。
   * 说明：如果请求的日志条目尚未提交，可能会延迟查询，直到日志条目应用完毕。
   * <p>
   * Query the state machine, provided minIndex <= commit index.
   * The request must be read-only.
   * Since the commit index of this server may lag behind the Raft service,
   * the returned result may possibly be stale.
   * <p>
   * When minIndex > {@link #getLastAppliedTermIndex()},
   * the state machine may choose to either
   * (1) return exceptionally, or
   * (2) wait until minIndex <= {@link #getLastAppliedTermIndex()} before running the query.
   */
  CompletableFuture<Message> queryStale(Message request, long minIndex);

  /**
   * 功能：启动一个事务，用于处理请求并准备将内容写入日志。
   * 说明：该方法用于创建一个新的事务并返回相关的 TransactionContext。
   * <p>
   * Start a transaction for the given request.
   * This method can be invoked in parallel when there are multiple requests.
   * The implementation should validate the request,
   * prepare a {@link StateMachineLogEntryProto},
   * and then build a {@link TransactionContext}.
   * The implementation should also be light-weighted.
   *
   * @return a transaction with the content to be written to the log.
   * @throws IOException thrown by the state machine while validation
   *
   * @see TransactionContext.Builder
   */
  TransactionContext startTransaction(RaftClientRequest request) throws IOException;

  /**
   *
   * Start a transaction for the given log entry for non-leaders.
   * This method can be invoked in parallel when there are multiple requests.
   * The implementation should prepare a {@link StateMachineLogEntryProto},
   * and then build a {@link TransactionContext}.
   * The implementation should also be light-weighted.
   *
   * @return a transaction with the content to be written to the log.
   */
  default TransactionContext startTransaction(LogEntryProto entry, RaftPeerRole role) {
    return TransactionContext.newBuilder()
        .setStateMachine(this)
        .setLogEntry(entry)
        .setServerRole(role)
        .build();
  }

  /**
   * 功能：在事务被追加到 Raft 日志之前调用，用于执行必要的操作。
   * 说明：该方法应该尽可能轻量，只做必须的操作。
   * @return The Transaction context.
   */
  TransactionContext preAppendTransaction(TransactionContext trx) throws IOException;

  /**
   * 功能：通知状态机事务无法追加到日志中，事务将被取消。
   * 说明：当事务无法同步时，应该调用此方法进行处理。
   * <p>
   * @param trx the transaction to cancel
   * @return cancelled transaction
   */
  TransactionContext cancelTransaction(TransactionContext trx) throws IOException;

  /**
   * 功能：用于提交到日志的事务，顺序执行，并返回结果。
   * 说明：该方法应顺序执行，并且只做必要的操作，避免并发执行。
   * @param trx the transaction state including the log entry that has been committed to a quorum
   *            of the raft peers
   * @return The Transaction context.
   */
  TransactionContext applyTransactionSerial(TransactionContext trx) throws InvalidProtocolBufferException;

  /**
   * 功能：提交已经复制到多数 Raft 节点的事务，返回结果消息。
   * 说明：这是一个异步方法，执行日志应用操作时，可能需要并发处理多个日志条目，但最终返回的结果要保证一致性。
   * <p>
   * 将已提交的日志条目应用到状态机。此方法按严格的顺序依次调用，确保事务的提交顺序与日志中的顺序一致。请注意，
   * 这个方法返回一个 `future`，是异步的。状态机的实现可以选择并行应用日志条目。在这种情况下，应用条目的顺序可能与日志提交的顺序不同。
   * <p>
   * 实现必须是确定性的，以确保 Raft 日志可以在任何 Raft 节点上重新播放。请注意，如果有三个或更多服务器，
   * Raft 算法确保即使某台机器出现硬件故障（或机器数量少于多数），日志仍然保持一致。
   * <p>
   * 任何在此方法中抛出的异常都将被视为不可恢复的错误（例如硬件故障）。发生此类错误时，服务器将关闭。
   * 管理员应手动修复底层问题，然后重新启动服务器。
   *
   * @param trx the transaction state including the log entry that has been replicated to a majority of the raft peers.
   *
   * @return a future containing the result message of the transaction,
   *         where the result message will be replied to the client.
   *         When there is an application level exception (e.g. access denied),
   *         the state machine may complete the returned future exceptionally.
   *         The exception will be wrapped in an {@link net.xdob.ratly.protocol.exceptions.StateMachineException}
   *         and then replied to the client.
   */
  CompletableFuture<Message> applyTransaction(TransactionContext trx);

  /**
   * 功能：返回状态机最后应用的日志条目的 term 和 index。
   * 说明：用于追踪状态机应用的日志条目。
   * @return the last term-index applied by this {@link StateMachine}.
   */
  TermIndex getLastAppliedTermIndex();

  /**
   * 功能：将给定的状态机日志条目 proto 转换为字符串表示。
   * 说明：该方法用于生成日志条目的字符串表示，可以帮助调试和日志记录。
   *
   * @param proto state machine proto
   * @return the string representation of the proto.
   */
  default String toStateMachineLogEntryString(StateMachineLogEntryProto proto) {
    return JavaUtils.getClassSimpleName(proto.getClass()) +  ":" + ClientInvocationId.valueOf(proto);
  }
}
