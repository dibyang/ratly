package net.xdob.ratly.server.impl;

import net.xdob.ratly.metrics.Timekeeper;
import net.xdob.ratly.proto.raft.RequestVoteReplyProto;
import net.xdob.ratly.proto.raft.RequestVoteRequestProto;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.DivisionInfo;
import net.xdob.ratly.server.RaftConfiguration;
import net.xdob.ratly.server.config.RaftServerConfigKeys;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.util.ServerStringUtils;
import com.google.common.annotations.VisibleForTesting;
import net.xdob.ratly.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Set;
import java.util.stream.Collectors;

import static net.xdob.ratly.util.LifeCycle.State.NEW;
import static net.xdob.ratly.util.LifeCycle.State.RUNNING;
import static net.xdob.ratly.util.LifeCycle.State.STARTING;

/**
 * 为候选人发起选举以成为领导者的过程。
 * 包含两个阶段：预投票（Pre-Vote）和正式选举（Election）。
 * <p>
 * 在预投票阶段，候选人不会改变自己的任期，而是尝试了解
 * 集群中是否有多数成员愿意将选票投给该候选人
 * （前提是候选人的日志足够最新，
 * 且选民在至少一个基准选举超时时间内未收到有效领导者的心跳）。
 * <p>
 * 一旦预投票通过，候选人将增加其任期并开始真正的选举。
 * <p>
 * 详见：
 * Ongaro, D. Consensus: Bridging Theory and Practice. PhD thesis, Stanford University, 2014.
 * Available at https://github.com/ongardie/dissertation
 */
class LeaderElection implements Runnable {
  public static final Logger LOG = LoggerFactory.getLogger(LeaderElection.class);
  public static final String FORCE_MODE = "force_mode";
  public static final String LEADER = "leader";

  private ResultAndTerm logAndReturn(Phase phase, Result result, Map<RaftPeerId, RequestVoteReplyProto> responses,
      List<Exception> exceptions) {
    return logAndReturn(phase, result, responses, exceptions, null);
  }

  private ResultAndTerm logAndReturn(Phase phase, Result result, Map<RaftPeerId, RequestVoteReplyProto> responses,
      List<Exception> exceptions, Long newTerm) {
    final ResultAndTerm resultAndTerm = new ResultAndTerm(result, newTerm);
    LOG.info("{}: {} {} received {} response(s) and {} exception(s):",
        this, phase, resultAndTerm, responses.size(), exceptions.size());
    int i = 0;
    for(RequestVoteReplyProto reply : responses.values()) {
      LOG.info("  Response {}: {}", i++, ServerStringUtils.toRequestVoteReplyString(reply));
    }
    for(Exception e : exceptions) {
      final int j = i++;
      LogUtils.infoOrTrace(LOG, () -> "  Exception " + j, e);
    }
    return resultAndTerm;
  }

  enum Phase {
    PRE_VOTE,
    ELECTION
  }

  enum Result {
    /**
     * 投票通过
     */
    PASSED,
    /**
     * 单节点模式下通过
     */
    SINGLE_MODE_PASSED,
    /**
     * 双节点模式下观察者辅助通过
     */
    ASSIST_PASSED,
    /**
     * 投票被拒绝
     */
    REJECTED,
    /**
     * 投票过程超时
     */
    TIMEOUT,
    /**
     * 在投票过程中发现了更高的任期（Term），当前投票终止。
     */
    DISCOVERED_A_NEW_TERM,
    /**
     * 节点关闭或集群停止，导致投票未能完成。
     */
    SHUTDOWN,
    /**
     * 投票节点不在当前的配置（Configuration）中，无法参与投票。
     */
    NOT_IN_CONF}

  private static class ResultAndTerm {
    private final Result result;
    private final Long term;

    ResultAndTerm(Result result, Long term) {
      this.result = result;
      this.term = term;
    }

    long maxTerm(long thatTerm) {
      return this.term != null && this.term > thatTerm ? this.term: thatTerm;
    }

    Result getResult() {
      return result;
    }

    @Override
    public String toString() {
      return result + (term != null? " (term=" + term + ")": "");
    }
  }

  static class Executor {
    private final ExecutorCompletionService<RequestVoteReplyProto> service;
    private final ExecutorService executor;

    private final AtomicInteger count = new AtomicInteger();

    Executor(Object name, int size) {
      Preconditions.assertTrue(size > 0);
      executor = Executors.newFixedThreadPool(size, r ->
          Daemon.newBuilder().setName(name + "-" + count.incrementAndGet()).setRunnable(r).build());
      service = new ExecutorCompletionService<>(executor);
    }

    void shutdown() {
      executor.shutdownNow();
    }

    void submit(Callable<RequestVoteReplyProto> task) {
      service.submit(task);
    }

    Future<RequestVoteReplyProto> poll(TimeDuration waitTime) throws InterruptedException {
      return service.poll(waitTime.getDuration(), waitTime.getUnit());
    }
  }

  static class ConfAndTerm {
    private final RaftConfiguration conf;
    private final long term;

    ConfAndTerm(RaftConfiguration conf, long term) {
      this.conf = conf;
      this.term = term;
    }

    long getTerm() {
      return term;
    }

    RaftConfiguration getConf() {
      return conf;
    }

    @Override
    public String toString() {
      return "term=" + term + ", " + conf;
    }
  }

  private static final AtomicInteger COUNT = new AtomicInteger();

  private final String name;
  private final LifeCycle lifeCycle;
  private final Daemon daemon;
  private final CompletableFuture<Void> stopped = new CompletableFuture<>();

  private final RaftServerImpl server;
  private final boolean skipPreVote;
  private final ConfAndTerm round0;

  LeaderElection(RaftServerImpl server, boolean force) {
    this.name = server.getMemberId() + "-" + JavaUtils.getClassSimpleName(getClass()) + COUNT.incrementAndGet();
    this.lifeCycle = new LifeCycle(this);
    this.daemon = Daemon.newBuilder().setName(name).setRunnable(this)
        .setThreadGroup(server.getThreadGroup()).build();
    this.server = server;
    this.skipPreVote = force ||
        !RaftServerConfigKeys.LeaderElection.preVote(
            server.getRaftServer().getProperties());
    try {
      // increase term of the candidate in advance if it's forced to election
      this.round0 = force ? server.getState().initElection(Phase.ELECTION) : null;
    } catch (IOException e) {
      throw new IllegalStateException(name + ": Failed to initialize election", e);
    }
  }

  void start() {
    startIfNew(daemon::start);
  }

  @VisibleForTesting
  void startInForeground() {
    startIfNew(this);
  }

  private void startIfNew(Runnable starter) {
    if (lifeCycle.compareAndTransition(NEW, STARTING)) {
      starter.run();
    } else {
      final LifeCycle.State state = lifeCycle.getCurrentState();
      LOG.info("{}: skip starting since this is already {}", this, state);
    }
  }

  CompletableFuture<Void> shutdown() {
    lifeCycle.checkStateAndClose();
    return stopped;
  }

  @VisibleForTesting
  LifeCycle.State getCurrentState() {
    return lifeCycle.getCurrentState();
  }

  @Override
  public void run() {
    try {
      runImpl();
    } finally {
      stopped.complete(null);
    }
  }

  private void runImpl() {
    if (!lifeCycle.compareAndTransition(STARTING, RUNNING)) {
      final LifeCycle.State state = lifeCycle.getCurrentState();
      LOG.info("{}: skip running since this is already {}", this, state);
      return;
    }

    try (AutoCloseable ignored = Timekeeper.start(server.getLeaderElectionMetrics().getLeaderElectionTimer())) {
      for (int round = 0; shouldRun(); round++) {
        if (skipPreVote || askForVotes(Phase.PRE_VOTE, round)) {
          if (askForVotes(Phase.ELECTION, round)) {
            server.changeToLeader();
          }
        }
      }
    } catch(Exception e) {
      if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
      }
      final LifeCycle.State state = lifeCycle.getCurrentState();
      if (state.isClosingOrClosed()) {
        LOG.info(this + ": since this is already " + state + ", safely ignore " + e);
      } else {
        if (!server.getInfo().isAlive()) {
          LOG.info(this + ": since the server is not alive, safely ignore " + e);
        } else {
          LOG.error("{}: Failed, state={}", this, state, e);
        }
        shutdown();
      }
    } finally {
      // Update leader election completion metric(s).
      server.getLeaderElectionMetrics().onNewLeaderElectionCompletion();
      lifeCycle.checkStateAndClose(() -> {});
    }
  }

  private boolean shouldRun() {
    final DivisionInfo info = server.getInfo();
    return lifeCycle.getCurrentState().isRunning() && info.isCandidate() && info.isAlive();
  }

  private boolean shouldRun(long electionTerm) {
    return shouldRun() && server.getState().getCurrentTerm() == electionTerm;
  }

  private ResultAndTerm submitRequestAndWaitResult(Phase phase, RaftConfiguration conf, long electionTerm)
      throws InterruptedException {
    if (!conf.containsInConf(server.getId())) {
      return new ResultAndTerm(Result.NOT_IN_CONF, electionTerm);
    }
    final ResultAndTerm r;
    final Collection<RaftPeer> others = new ArrayList<>(conf.getOtherPeers(server.getId()));
    if (others.isEmpty()) {
      r = new ResultAndTerm(Result.PASSED, electionTerm);
    } else {
      final TermIndex lastEntry = server.getState().getLastEntry();
      final Executor voteExecutor = new Executor(this, others.size());
      try {
        final int submitted = submitRequests(phase, electionTerm, lastEntry, others, voteExecutor);
        r = waitForResults(phase, electionTerm, submitted, conf, voteExecutor, lastEntry);
      } finally {
        voteExecutor.shutdown();
      }
    }

    return r;
  }

  /** Send requestVote rpc to all other peers for the given phase. */
  private boolean askForVotes(Phase phase, int round) throws InterruptedException, IOException {
    final long electionTerm;
    final RaftConfiguration conf;
    synchronized (server) {
      if (!shouldRun()) {
        return false;
      }
      // 如果 round0 为非 null，则我们已经在构造函数中调用了 initElection，
      // 重复使用 round0 以避免在第一轮中再次使用 initElection
      final ConfAndTerm confAndTerm = (round == 0 && round0 != null) ?
          round0 : server.getState().initElection(phase);
      electionTerm = confAndTerm.getTerm();
      conf = confAndTerm.getConf();
    }

    LOG.info("{} {} round {}: submit vote requests at term {} for {}", this, phase, round, electionTerm, conf);
    final ResultAndTerm r = submitRequestAndWaitResult(phase, conf, electionTerm);
    LOG.info("{} {} round {}: result {}", this, phase, round, r);

    synchronized (server) {
      if (!shouldRun(electionTerm)) {
        return false; // term already passed or this should not run anymore.
      }

      switch (r.getResult()) {
        case PASSED:
        case SINGLE_MODE_PASSED:
        case ASSIST_PASSED:
          return true;
        case NOT_IN_CONF:
        case SHUTDOWN:
          server.close();
          server.getStateMachine().event().notifyServerShutdown(server.getRoleInfoProto(), false);
          return false;
        case TIMEOUT:
          return false; // should retry
        case REJECTED:
        case DISCOVERED_A_NEW_TERM:
          final long term = r.maxTerm(server.getState().getCurrentTerm());
          server.changeToFollowerAndPersistMetadata(term, false, r);
          return false;
        default: throw new IllegalArgumentException("Unable to process result " + r.result);
      }
    }
  }

  private int submitRequests(Phase phase, long electionTerm, TermIndex lastEntry,
      Collection<RaftPeer> others, Executor voteExecutor) {
    int submitted = 0;
    for (final RaftPeer peer : others) {
      final RequestVoteRequestProto r = ServerProtoUtils.toRequestVoteRequestProto(
          server.getMemberId(), peer.getId(), electionTerm, lastEntry, phase == Phase.PRE_VOTE);
      voteExecutor.submit(() -> server.getServerRpc().requestVote(r));
      submitted++;
    }
    return submitted;
  }

  /**
   * 获取高优先级节点id列表
   * 高于侯选人的优先级的节点id列表（以FOLLOWER启动的节点）
   */
  private Set<RaftPeerId> getHigherPriorityPeers(RaftConfiguration conf) {
    //获取侯选人的优先级
    final Optional<Integer> priority = Optional.ofNullable(conf.getPeer(server.getId()))
        .map(RaftPeer::getPriority);
    //获取高于侯选人的优先级的节点（以FOLLOWER启动的节点）
    return conf.getAllPeers().stream()
        .filter(peer -> priority.filter(p -> peer.getPriority() > p).isPresent())
        .map(RaftPeer::getId)
        .collect(Collectors.toSet());
  }

  private ResultAndTerm waitForResults(Phase phase, long electionTerm, int submitted,
      RaftConfiguration conf, Executor voteExecutor, TermIndex lastEntry) throws InterruptedException {
    TimeDuration randomElectionTimeout = server.getRandomElectionTimeout();

    final Timestamp timeout = Timestamp.currentTime().addTime(randomElectionTimeout);
    final Map<RaftPeerId, RequestVoteReplyProto> responses = new HashMap<>();
    final List<Exception> exceptions = new ArrayList<>();
    int waitForNum = submitted;
    Collection<RaftPeerId> votedPeers = new ArrayList<>();
    Collection<RaftPeerId> rejectedPeers = new ArrayList<>();
    Set<RaftPeerId> higherPriorityPeers = getHigherPriorityPeers(conf);
    final boolean singleMode = conf.isSingleMode(server.getId());
    int waitMS = 200;
    while (waitForNum > 0 && shouldRun(electionTerm)) {
      final TimeDuration waitTime = timeout.elapsedTime().apply(n -> -n);
      //超过等待时间
      if (waitTime.isNonPositive()) {
        if (conf.hasMajority(votedPeers, server.getId())) {
          // 如果某个优先级较高的对等节点在超时时没有响应，但候选节点获得多数，
          // 候选人通过投票
          return logAndReturn(phase, Result.PASSED, responses, exceptions);
        }else if (singleMode) {
          // 如果 Candidate 处于单节点模式，则 Candidate 通过 Vote。
          return logAndReturn(phase, Result.SINGLE_MODE_PASSED, responses, exceptions);
        }else {
          return logAndReturn(phase, Result.TIMEOUT, responses, exceptions);
        }
      }

      try {
        final Future<RequestVoteReplyProto> future = voteExecutor.poll(waitTime);
        if (future == null) {
          continue; // poll timeout, continue to return Result.TIMEOUT
        }

        final RequestVoteReplyProto r = future.get();
        final RaftPeerId replierId = RaftPeerId.valueOf(r.getServerReply().getReplyId());
        final RequestVoteReplyProto previous = responses.putIfAbsent(replierId, r);
        if (previous != null) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("{} received duplicated replies from {}, the 2nd reply is ignored: 1st={}, 2nd={}",
                this, replierId,
                ServerStringUtils.toRequestVoteReplyString(previous),
                ServerStringUtils.toRequestVoteReplyString(r));
          }
          continue;
        }
        if (r.getShouldShutdown()) {
          return logAndReturn(phase, Result.SHUTDOWN, responses, exceptions);
        }
        if (r.getTerm() > electionTerm) {
          return logAndReturn(phase, Result.DISCOVERED_A_NEW_TERM, responses, exceptions, r.getTerm());
        }

        // 如果任何具有更高优先级的对等体投拒绝票，则候选人不能通过投票
        if (!r.getServerReply().getSuccess() && higherPriorityPeers.contains(replierId) && !singleMode) {
          return logAndReturn(phase, Result.REJECTED, responses, exceptions);
        }

        // 删除更高优先级的 peer，以便我们检查 higherPriorityPeers 为空以确保
        // 所有优先级较高的对等体都已回复
        higherPriorityPeers.remove(replierId);

        if (r.getServerReply().getSuccess()) {
          votedPeers.add(replierId);
          // 如果大多数和所有具有较高优先级的对等体都已投票，则候选人通过投票
          if (higherPriorityPeers.isEmpty() && conf.hasMajority(votedPeers, server.getId())) {
            return logAndReturn(phase, Result.PASSED, responses, exceptions);
          }
        } else {
          rejectedPeers.add(replierId);
          if (conf.majorityRejectVotes(rejectedPeers)) {
            LOG.warn("majorityRejectVotes is true, phase={}, electionTerm={}", phase, electionTerm);
            return logAndReturn(phase, Result.REJECTED, responses, exceptions);
          }
        }
      } catch(ExecutionException e) {
        LogUtils.infoOrTrace(LOG, () -> this + " got exception when requesting votes", e);
        exceptions.add(e);
      }
      waitForNum--;
    }
    // 收到所有回复
    if (conf.hasMajority(votedPeers, server.getId())) {
      return logAndReturn(phase, Result.PASSED, responses, exceptions);
    } else if (singleMode) {
      return logAndReturn(phase, Result.SINGLE_MODE_PASSED, responses, exceptions);
    } else {
      boolean forceLeader = isForceMode();
      LOG.warn("forceLeader={}, phase={}, electionTerm={}", forceLeader, phase, electionTerm);
      if(forceLeader){
        LOG.warn("force election this node leader phase={}, electionTerm={}", phase, electionTerm);
        //强制以leader启动
        return logAndReturn(phase, Result.ASSIST_PASSED, responses, exceptions);
      }
      return logAndReturn(phase, Result.REJECTED, responses, exceptions);
    }
  }

  /**
   * 是否强制以leader启动
   * @return 是否强制以leader启动
   */
  public static boolean isForceMode() {
    return LEADER.equals(System.getProperty(FORCE_MODE, ""));
  }

  @Override
  public String toString() {
    return name;
  }

  /**
   * 清除强制启动标志
   */
  public static void cleanForceMode(){
    //清除强制启动标志
    System.clearProperty(FORCE_MODE);
  }
}
