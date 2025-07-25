package net.xdob.ratly.server.impl;

import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.proto.raft.*;
import net.xdob.ratly.protocol.ClientId;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.protocol.TransferLeadershipRequest;
import net.xdob.ratly.protocol.exceptions.TransferLeadershipException;
import net.xdob.ratly.server.config.RaftServerConfigKeys;
import net.xdob.ratly.server.leader.FollowerInfo;
import net.xdob.ratly.server.leader.LogAppender;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.MemoizedSupplier;
import net.xdob.ratly.util.StringUtils;
import net.xdob.ratly.util.TimeDuration;
import net.xdob.ratly.util.TimeoutExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class TransferLeadership {
  public static final Logger LOG = LoggerFactory.getLogger(TransferLeadership.class);

  private static class Context {
    private final TransferLeadershipRequest request;
    private final Supplier<LogAppender> transferee;

    Context(TransferLeadershipRequest request, Supplier<LogAppender> transferee) {
      this.request = request;
      this.transferee = transferee;
    }

    TransferLeadershipRequest getRequest() {
      return request;
    }

    RaftPeerId getTransfereeId() {
      return request.getNewLeader();
    }

    LogAppender getTransfereeLogAppender() {
      return transferee.get();
    }
  }

  static class Result {
    enum Type {
      SUCCESS,
      DIFFERENT_LEADER,
      NULL_FOLLOWER,
      NULL_LOG_APPENDER,
      NOT_UP_TO_DATE,
      TIMED_OUT,
      FAILED_TO_START,
      COMPLETED_EXCEPTIONALLY,
    }

    static final Result SUCCESS = new Result(Type.SUCCESS);
    static final Result DIFFERENT_LEADER = new Result(Type.DIFFERENT_LEADER);
    static final Result NULL_FOLLOWER = new Result(Type.NULL_FOLLOWER);
    static final Result NULL_LOG_APPENDER = new Result(Type.NULL_LOG_APPENDER);

    private final Type type;
    private final String errorMessage;
    private final Throwable exception;

    private Result(Type type) {
      this(type, null);
    }

    private Result(Type type, String errorMessage, Throwable exception) {
      this.type = type;
      this.errorMessage = errorMessage;
      this.exception = exception;
    }

    Result(Type type, String errorMessage) {
      this(type, errorMessage, null);
    }

    Result(Throwable t) {
      this(Type.COMPLETED_EXCEPTIONALLY, null, t);
    }

    Type getType() {
      return type;
    }

    @Override
    public String toString() {
      if (exception == null) {
        return type + (errorMessage == null ? "" : "(" + errorMessage + ")");
      }
      return type + ": " + StringUtils.stringifyException(exception);
    }
  }

  class PendingRequest {
    private final TransferLeadershipRequest request;
    private final CompletableFuture<RaftClientReply> replyFuture = new CompletableFuture<>();

    PendingRequest(TransferLeadershipRequest request) {
      this.request = request;
    }

    TransferLeadershipRequest getRequest() {
      return request;
    }

    CompletableFuture<RaftClientReply> getReplyFuture() {
      return replyFuture;
    }

    void complete(Result result) {
      if (replyFuture.isDone()) {
        return;
      }
      final RaftPeerId currentLeader = server.getState().getLeaderId();
      if (currentLeader != null && currentLeader.equals(request.getNewLeader())) {
        replyFuture.complete(server.newSuccessReply(request));
      } else {
        if (result.getType() == Result.Type.SUCCESS) {
          result = Result.DIFFERENT_LEADER;
        }
        final TransferLeadershipException tle = new TransferLeadershipException(server.getMemberId()
            + ": Failed to transfer leadership to " + request.getNewLeader()
            + " (the current leader is " + currentLeader + "): " + result);
        replyFuture.complete(server.newExceptionReply(request, tle));
      }
    }

    @Override
    public String toString() {
      return request.toString();
    }
  }

  private final RaftServerImpl server;
  private final TimeDuration requestTimeout;
  private final TimeoutExecutor scheduler = TimeoutExecutor.getInstance();

  private final AtomicReference<PendingRequest> pending = new AtomicReference<>();

  TransferLeadership(RaftServerImpl server, RaftProperties properties) {
    this.server = server;
    this.requestTimeout = RaftServerConfigKeys.Rpc.requestTimeout(properties);
  }

  private Optional<RaftPeerId> getTransferee() {
    return Optional.ofNullable(pending.get())
        .map(r -> r.getRequest().getNewLeader());
  }

  boolean isSteppingDown() {
    return pending.get() != null;
  }

  static Result isFollowerUpToDate(FollowerInfo follower, TermIndex leaderLastEntry) {
    if (follower == null) {
      return Result.NULL_FOLLOWER;
    }
    if (leaderLastEntry == null) {
      // The transferee is expecting leaderLastEntry to be non-null,
      // return NOT_UP_TO_DATE to indicate TransferLeadership should wait.
      return new Result(Result.Type.NOT_UP_TO_DATE, "leaderLastEntry is null");
    }
    final long followerMatchIndex = follower.getMatchIndex();
    if (followerMatchIndex < leaderLastEntry.getIndex()) {
      return new Result(Result.Type.NOT_UP_TO_DATE, "followerMatchIndex = " + followerMatchIndex
          + " < leaderLastEntry.getIndex() = " + leaderLastEntry.getIndex());
    }
    return Result.SUCCESS;
  }

  /**
   * 向指定的跟随者发送领导选举请求，以实现领导权转移。其核心逻辑如下：
   *  1. 获取当前服务器的最后一条日志条目 lastEntry。
   *  2. 检查目标跟随者是否已同步最新数据（通过 isFollowerUpToDate）；若未同步，则返回相应错误结果。
   *  3. 构建 StartLeaderElectionRequestProto 请求对象，包含当前服务器ID、目标跟随者ID及最后日志条目。
   *  4. 异步发送领导选举请求，并记录指标和日志。
   *  5. 若异步调用已异常完成，捕获异常并返回封装后的错误结果。
   *  6. 否则返回 Result.SUCCESS 表示请求已成功发起。
   */
  private Result sendStartLeaderElection(FollowerInfo follower) {
    final TermIndex lastEntry = server.getState().getLastEntry();

    final Result result = isFollowerUpToDate(follower, lastEntry);
    if (result != Result.SUCCESS) {
      return result;
    }

    final RaftPeerId transferee = follower.getId();
    LOG.info("{}: sendStartLeaderElection to follower {}, lastEntry={}",
        server.getMemberId(), transferee, lastEntry);

    final StartLeaderElectionRequestProto r = ServerProtoUtils.toStartLeaderElectionRequestProto(
        server.getMemberId(), transferee, lastEntry);
    final CompletableFuture<StartLeaderElectionReplyProto> f = CompletableFuture.supplyAsync(() -> {
      server.getLeaderElectionMetrics().onTransferLeadership();
      try {
        return server.getServerRpc().startLeaderElection(r);
      } catch (IOException e) {
        throw new CompletionException("Failed to sendStartLeaderElection to follower " + transferee, e);
      }
    }, server.getServerExecutor()).whenComplete((reply, exception) -> {
      if (reply != null) {
        LOG.info("{}: Received startLeaderElection reply from {}: success? {}",
            server.getMemberId(), transferee, reply.getServerReply().getSuccess());
      } else if (exception != null) {
        LOG.warn(server.getMemberId() + ": Failed to startLeaderElection for " + transferee, exception);
      }
    });

    if (f.isCompletedExceptionally()) { // already failed
      try {
        f.join();
      } catch (Throwable t) {
        return new Result(t);
      }
    }
    return Result.SUCCESS;
  }

  /**
   * If the transferee has just append some entries and becomes up-to-date,
   * send StartLeaderElection to it
   */
  void onFollowerAppendEntriesReply(FollowerInfo follower) {
    if (!getTransferee().filter(t -> t.equals(follower.getId())).isPresent()) {
      return;
    }
    final Result result = sendStartLeaderElection(follower);
    if (result == Result.SUCCESS) {
      LOG.info("{}: sent StartLeaderElection to transferee {} after received AppendEntriesResponse",
          server.getMemberId(), follower.getId());
    }
  }

  /**
   * 尝试将领导权转移给指定的跟随者（transferee）。其逻辑如下：
   *  1. 获取待转移节点的 ID 和日志追加器（LogAppender）。
   *  2. 如果 LogAppender 为空，返回错误结果。
   *  3. 获取对应的 FollowerInfo 并发送 StartLeaderElection 请求。
   *  4. 根据发送结果：
   *     若成功，记录日志表示目标节点已拥有最新日志。
   *     若失败且因日志未同步，则通知 LogAppender 发送 AppendEntries 以追赶日志。
   */
  private Result tryTransferLeadership(Context context) {
    final RaftPeerId transferee = context.getTransfereeId();
    LOG.info("{}: start transferring leadership to {}", server.getMemberId(), transferee);
    final LogAppender appender = context.getTransfereeLogAppender();
    if (appender == null) {
      return Result.NULL_LOG_APPENDER;
    }
    final FollowerInfo follower = appender.getFollower();
    final Result result = sendStartLeaderElection(follower);
    if (result.getType() == Result.Type.SUCCESS) {
      LOG.info("{}: {} sent StartLeaderElection to transferee {} immediately as it already has up-to-date log",
          server.getMemberId(), result, transferee);
    } else if (result.getType() == Result.Type.NOT_UP_TO_DATE) {
      LOG.info("{}: {} notifying LogAppender to send AppendEntries to transferee {}",
          server.getMemberId(), result, transferee);
      appender.notifyLogAppender();
    }
    return result;
  }

  void start(LogAppender transferee) {
    // TransferLeadership will block client request, so we don't want wait too long.
    // If everything goes well, transferee should be elected within the min rpc timeout.
    final long timeout = server.properties().minRpcTimeoutMs();
    final TransferLeadershipRequest request = new TransferLeadershipRequest(ClientId.emptyClientId(),
        server.getId(), server.getMemberId().getGroupId(), 0, transferee.getFollowerId(), timeout);
    start(new Context(request, () -> transferee));
  }

  CompletableFuture<RaftClientReply> start(LeaderStateImpl leaderState, TransferLeadershipRequest request) {
    final Context context = new Context(request,
        JavaUtils.memoize(() -> leaderState.getLogAppender(request.getNewLeader()).orElse(null)));
    return start(context);
  }

  /**
   * 主要功能如下：
   * 获取或创建一个用于处理当前请求的 PendingRequest。
   * 如果已有挂起的请求，则基于已有请求返回结果。
   * 在转移前禁用租约（lease）。
   * 尝试进行领导权转移，根据尝试结果决定是否完成请求或设置超时。
   * 若转移失败，在最终完成后恢复之前的租约状态。
   */
  private CompletableFuture<RaftClientReply> start(Context context) {
    final TransferLeadershipRequest request = context.getRequest();
    final MemoizedSupplier<PendingRequest> supplier = JavaUtils.memoize(() -> new PendingRequest(request));
    final PendingRequest previous = pending.getAndUpdate(f -> f != null? f: supplier.get());
    if (previous != null) {
      return createReplyFutureFromPreviousRequest(request, previous);
    }
    // disable the lease before transferring leader
    final boolean previousLeaseEnabled = server.getRole().getLeaderState()
        .map(l -> l.getAndSetLeaseEnabled(false)).orElse(false);
    final PendingRequest pendingRequest = supplier.get();
    final Result result = tryTransferLeadership(context);
    final Result.Type type = result.getType();
    if (type != Result.Type.SUCCESS && type != Result.Type.NOT_UP_TO_DATE) {
      pendingRequest.complete(result);
    } else {
      // if timeout is not specified in request, use default request timeout
      final TimeDuration timeout = request.getTimeoutMs() == 0 ? requestTimeout
          : TimeDuration.valueOf(request.getTimeoutMs(), TimeUnit.MILLISECONDS);
      scheduler.onTimeout(timeout, () -> complete(new Result(Result.Type.TIMED_OUT,
              timeout.toString(TimeUnit.SECONDS, 3))),
          LOG, () -> "Failed to handle timeout");
    }
    // reset back lease if the current transfer fails
    pendingRequest.getReplyFuture().whenCompleteAsync((reply, ex) -> {
      if (ex != null || !reply.isSuccess()) {
        server.getRole().getLeaderState().ifPresent(l -> l.getAndSetLeaseEnabled(previousLeaseEnabled));
      }
    });
    return pendingRequest.getReplyFuture();
  }

  private CompletableFuture<RaftClientReply> createReplyFutureFromPreviousRequest(
      TransferLeadershipRequest request, PendingRequest previous) {
    if (request.getNewLeader().equals(previous.getRequest().getNewLeader())) {
      final CompletableFuture<RaftClientReply> replyFuture = new CompletableFuture<>();
      previous.getReplyFuture().whenComplete((r, e) -> {
        if (e != null) {
          replyFuture.completeExceptionally(e);
        } else {
          replyFuture.complete(r.isSuccess() ? server.newSuccessReply(request)
              : server.newExceptionReply(request, r.getException()));
        }
      });
      return replyFuture;
    } else {
      final TransferLeadershipException tle = new TransferLeadershipException(server.getMemberId() +
          "Failed to transfer leadership to " + request.getNewLeader() + ": a previous " + previous + " exists");
      return CompletableFuture.completedFuture(server.newExceptionReply(request, tle));
    }
  }

  void complete(Result result) {
    Optional.ofNullable(pending.getAndSet(null))
        .ifPresent(r -> r.complete(result));
  }
}
