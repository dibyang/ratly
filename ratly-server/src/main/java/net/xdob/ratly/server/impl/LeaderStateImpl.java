package net.xdob.ratly.server.impl;

import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.proto.raft.*;
import net.xdob.ratly.proto.raft.AppendEntriesRequestProto;
import net.xdob.ratly.proto.raft.CommitInfoProto;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.proto.raft.LogEntryProto.LogEntryBodyCase;
import net.xdob.ratly.proto.raft.RaftPeerRole;
import net.xdob.ratly.proto.raft.ReplicationLevel;
import net.xdob.ratly.proto.raft.RoleInfoProto;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftClientRequest;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.protocol.SetConfigurationRequest;
import net.xdob.ratly.protocol.TransferLeadershipRequest;
import net.xdob.ratly.protocol.exceptions.LeaderNotReadyException;
import net.xdob.ratly.protocol.exceptions.NotLeaderException;
import net.xdob.ratly.protocol.exceptions.NotReplicatedException;
import net.xdob.ratly.protocol.exceptions.ReadIndexException;
import net.xdob.ratly.protocol.exceptions.ReconfigurationTimeoutException;
import net.xdob.ratly.server.CommitInfoCache;
import net.xdob.ratly.server.PeerConfiguration;
import net.xdob.ratly.server.RaftConfiguration;
import net.xdob.ratly.server.RaftConfigurationImpl;
import net.xdob.ratly.server.config.RaftServerConfigKeys;
import net.xdob.ratly.server.impl.ReadIndexHeartbeats.AppendEntriesListener;
import net.xdob.ratly.server.leader.FollowerInfo;
import net.xdob.ratly.server.leader.LeaderState;
import net.xdob.ratly.server.leader.LogAppender;
import net.xdob.ratly.server.metrics.LogAppenderMetrics;
import net.xdob.ratly.server.metrics.RaftServerMetricsImpl;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.LogEntryHeader;
import net.xdob.ratly.server.raftlog.LogProtoUtils;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.statemachine.TransactionContext;
import net.xdob.ratly.util.CodeInjectionForTesting;
import net.xdob.ratly.util.Collections3;
import net.xdob.ratly.util.Daemon;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.MemoizedSupplier;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.ReferenceCountedObject;
import net.xdob.ratly.util.TimeDuration;
import net.xdob.ratly.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static net.xdob.ratly.server.config.RaftServerConfigKeys.Write.FOLLOWER_GAP_RATIO_MAX_KEY;

/**
 * 仅为领导者的状态。它包含三种不同类型的处理器：
 * 1. RPC 发送器：每个线程都在向一个跟随者追加日志
 * 2. 事件处理器（EventProcessor）：一个单独的线程，根据日志追加响应的状态更新 Raft 服务器的状态
 * 3. 待处理请求处理器（PendingRequestHandler）：当对应的日志条目被提交时，将响应返回给客户端的处理器
 */
class LeaderStateImpl implements LeaderState {
  public static final String APPEND_PLACEHOLDER = JavaUtils.getClassSimpleName(LeaderState.class) + ".placeholder";
  static final Logger LOG = LoggerFactory.getLogger(LeaderStateImpl.class);
  private enum BootStrapProgress {
    NOPROGRESS, PROGRESSING, CAUGHTUP
  }

  static class StateUpdateEvent {
    private enum Type {
      STEP_DOWN, UPDATE_COMMIT, CHECK_STAGING
    }

    private final Type type;
    private final long newTerm;
    private final Runnable handler;

    StateUpdateEvent(Type type, long newTerm, Runnable handler) {
      this.type = type;
      this.newTerm = newTerm;
      this.handler = handler;
    }

    void execute() {
      handler.run();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (!(obj instanceof StateUpdateEvent)) {
        return false;
      }
      final StateUpdateEvent that = (StateUpdateEvent)obj;
      return this.type == that.type && this.newTerm == that.newTerm;
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, newTerm);
    }

    @Override
    public String toString() {
      return type + (newTerm >= 0? ":" + newTerm: "");
    }
  }

  private class EventQueue {
    private final String name = server.getMemberId() + "-" + JavaUtils.getClassSimpleName(getClass());
    private final BlockingQueue<StateUpdateEvent> queue = new ArrayBlockingQueue<>(4096);

    void submit(StateUpdateEvent event) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        LOG.info("{}: Interrupted when submitting {} ", this, event);
        Thread.currentThread().interrupt();
      }
    }

    StateUpdateEvent poll() {
      final StateUpdateEvent e;
      try {
        e = queue.poll(server.getMaxTimeoutMs(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        String s = this + ": poll() is interrupted";
        if (isStopped.get()) {
          LOG.info(s + " gracefully");
          return null;
        } else {
          throw new IllegalStateException(s + " UNEXPECTEDLY", ie);
        }
      }

      if (e != null) {
        // remove duplicated events from the head.
        while(e.equals(queue.peek())) {
          queue.poll();
        }
      }
      return e;
    }

    @Override
    public String toString() {
      return name;
    }
  }


  /**
   * 使用 {@link CopyOnWriteArrayList} 实现一个线程安全的列表。
   * 由于每次修改都会导致列表的复制，因此只支持批量操作
   * （addAll 和 removeAll）。
   */
  static class SenderList implements Iterable<LogAppender> {
    private final List<LogAppender> senders;
    SenderList() {
      this.senders = new CopyOnWriteArrayList<>();
    }

    @Override
    public Iterator<LogAppender> iterator() {
      return senders.stream().iterator();
    }

    void addAll(Collection<LogAppender> newSenders) {
      if (newSenders.isEmpty()) {
        return;
      }

      Preconditions.assertUnique(
          Collections3.as(senders, LogAppender::getFollowerId),
          Collections3.as(newSenders, LogAppender::getFollowerId));

      final boolean changed = senders.addAll(newSenders);
      Preconditions.assertTrue(changed);
    }

    boolean removeAll(Collection<LogAppender> c) {
      return senders.removeAll(c);
    }

    CompletableFuture<Void> stopAll() {
      return CompletableFuture.allOf(senders.stream().
              map(LogAppender::stopAsync).toArray(CompletableFuture[]::new));
    }
  }

  /** For caching {@link FollowerInfo}s.  This class is immutable. */
  static class CurrentOldFollowerInfos {
    private final RaftConfiguration conf;
    private final List<FollowerInfo> current;
    private final List<FollowerInfo> old;

    CurrentOldFollowerInfos(RaftConfiguration conf, List<FollowerInfo> current, List<FollowerInfo> old) {
      // set null when the sizes are not the same so that it will update next time.
      this.conf = isSameSize(current, conf.getConf()) && isSameSize(old, conf.getOldConf())? conf: null;
      this.current = Collections.unmodifiableList(current);
      this.old = old == null? null: Collections.unmodifiableList(old);
    }

    RaftConfiguration getConf() {
      return conf;
    }

    List<FollowerInfo> getCurrent() {
      return current;
    }

    List<FollowerInfo> getOld() {
      return old;
    }
  }

  static boolean isSameSize(List<FollowerInfo> infos, PeerConfiguration conf) {
    return conf == null? infos == null: conf.size() == infos.size();
  }

  /** Use == to compare if the confs are the same object. */
  static boolean isSameConf(CurrentOldFollowerInfos cached, RaftConfiguration conf) {
    return cached != null && cached.getConf() == conf;
  }

  static class FollowerInfoMap {
    private final Map<RaftPeerId, FollowerInfo> map = new ConcurrentHashMap<>();
    @SuppressWarnings({"squid:S3077"}) // Suppress volatile for generic type
    private volatile CurrentOldFollowerInfos followerInfos;

    void put(RaftPeerId id, FollowerInfo info) {
      map.put(id, info);
    }

    CurrentOldFollowerInfos getFollowerInfos(RaftConfiguration conf) {
      final CurrentOldFollowerInfos cached = followerInfos;
      if (isSameConf(cached, conf)) {
        return cached;
      }

      return update(conf);
    }

    synchronized CurrentOldFollowerInfos update(RaftConfiguration conf) {
      if (!isSameConf(followerInfos, conf)) { // compare again synchronized
        followerInfos = new CurrentOldFollowerInfos(conf, getFollowerInfos(conf.getConf()),
            Optional.ofNullable(conf.getOldConf()).map(this::getFollowerInfos).orElse(null));
      }
      return followerInfos;
    }

    private List<FollowerInfo> getFollowerInfos(PeerConfiguration peers) {
      return peers.streamPeerIds().map(map::get).filter(Objects::nonNull).collect(Collectors.toList());
    }
  }

  /**
   * 该内部类 StartupLogEntry 用于记录领导者启动时的日志索引和控制领导者是否就绪的状态。
   *   1. startIndex：在Leader启动时，将当前配置写入日志，并记录该日志条目的索引。
   *   2. appliedIndexFuture：未来会在该日志被应用后完成此 Future。
   *   3. getAppliedIndexFuture()：返回该 Future，供外部等待日志被应用。
   *   4. checkStartIndex()：判断传入的日志条目是否为启动日志条目，若是，则完成 Future 并标记 Leader 已就绪。
   *   5. isApplied()：通过 JavaUtils.isCompletedNormally() 检查日志是否已正常应用。
   */
  private class StartupLogEntry {
    /** The log index at leader startup. */
    private final long startIndex = appendConfiguration(RaftConfigurationImpl.newBuilder()
        .setConf(server.getRaftConf().getConf())
        .setLogEntryIndex(raftLog.getNextIndex())
        .build());
    /** This future will be completed after the log entry is applied. */
    private final CompletableFuture<Long> appliedIndexFuture = new CompletableFuture<>();

    CompletableFuture<Long> getAppliedIndexFuture() {
      return appliedIndexFuture;
    }

    boolean checkStartIndex(LogEntryProto logEntry) {
      final boolean completed = logEntry.getIndex() == startIndex && appliedIndexFuture.complete(startIndex);
      if (completed) {
        LOG.info("Leader {} is ready since appliedIndex == startIndex == {}", LeaderStateImpl.this, startIndex);
      }else{
        LOG.info("Leader {} is not ready for startIndex = {}, log Index = {}", LeaderStateImpl.this, startIndex, logEntry.getIndex());
      }
      return completed;
    }

    boolean isApplied() {
      return JavaUtils.isCompletedNormally(appliedIndexFuture);
    }
  }

  private final StateUpdateEvent updateCommitEvent =
      new StateUpdateEvent(StateUpdateEvent.Type.UPDATE_COMMIT, -1, this::updateCommit);
  private final StateUpdateEvent checkStagingEvent =
      new StateUpdateEvent(StateUpdateEvent.Type.CHECK_STAGING, -1, this::checkStaging);

  private final String name;
  private final RaftServerImpl server;
  private final RaftLog raftLog;
  private final long currentTerm;
  @SuppressWarnings({"squid:S3077"}) // Suppress volatile for generic type
  private volatile ConfigurationStagingState stagingState;

  private final FollowerInfoMap followerInfoMap = new FollowerInfoMap();


  /**
   * 一组用于向跟随者追加日志条目的线程列表。
   * 该列表由 RaftServer 的锁保护。
   */
  private final SenderList senders;
  private final EventQueue eventQueue;
  private final EventProcessor processor;
  private final PendingRequests pendingRequests;
  private final WatchRequests watchRequests;
  private final MessageStreamRequests messageStreamRequests;

  private final MemoizedSupplier<StartupLogEntry> startupLogEntry = MemoizedSupplier.valueOf(StartupLogEntry::new);
  private final AtomicBoolean isStopped = new AtomicBoolean();

  private final boolean logMetadataEnabled;
  private final int stagingCatchupGap;
  private final RaftServerMetricsImpl raftServerMetrics;
  private final LogAppenderMetrics logAppenderMetrics;
  private final long followerMaxGapThreshold;
  private final PendingStepDown pendingStepDown;

  private final ReadIndexHeartbeats readIndexHeartbeats;
  private final LeaderLease lease;
  private final RaftPeerId leaderId;
  private final AtomicReference<String> vnPeerId = new AtomicReference<>();

  LeaderStateImpl(RaftServerImpl server) {
    this.name = server.getMemberId() + "-" + JavaUtils.getClassSimpleName(getClass());
    this.server = server;
    this.leaderId = server.getId();
    final RaftProperties properties = server.getRaftServer().getProperties();
    stagingCatchupGap = RaftServerConfigKeys.stagingCatchupGap(properties);

    final ServerState state = server.getState();
    this.raftLog = state.getLog();
    this.currentTerm = state.getCurrentTerm();

    this.eventQueue = new EventQueue();
    processor = new EventProcessor(this.name, server);
    raftServerMetrics = server.getRaftServerMetrics();
    logAppenderMetrics = new LogAppenderMetrics(server.getMemberId());
    this.pendingRequests = new PendingRequests(server.getMemberId(), properties, raftServerMetrics);
    this.watchRequests = new WatchRequests(server.getMemberId(), properties, raftServerMetrics);
    this.messageStreamRequests = new MessageStreamRequests(server.getMemberId());
    this.pendingStepDown = new PendingStepDown(this);
    this.readIndexHeartbeats = new ReadIndexHeartbeats();
    this.lease = new LeaderLease(properties);
    this.logMetadataEnabled = RaftServerConfigKeys.Log.logMetadataEnabled(properties);
    long maxPendingRequests = RaftServerConfigKeys.Write.elementLimit(properties);
    double followerGapRatioMax = RaftServerConfigKeys.Write.followerGapRatioMax(properties);

    if (followerGapRatioMax == -1) {
      this.followerMaxGapThreshold = -1;
    } else if (followerGapRatioMax > 1f || followerGapRatioMax <= 0f) {
      throw new IllegalArgumentException(FOLLOWER_GAP_RATIO_MAX_KEY +
          "s value must between [1, 0) to enable the feature");
    } else {
      this.followerMaxGapThreshold = (long) (followerGapRatioMax * maxPendingRequests);
    }

    final RaftConfiguration conf = state.getRaftConf();
    Collection<RaftPeer> others = conf.getOtherPeers(server.getId());

    final long nextIndex = raftLog.getNextIndex();
    senders = new SenderList();
    addSenders(others, nextIndex, true);

    final Collection<RaftPeer> listeners = conf.getAllPeers(RaftPeerRole.LISTENER);
    if (!listeners.isEmpty()) {
      addSenders(listeners, nextIndex, true);
    }
  }

  void start() {
    // 在新任期开始时，复制一个配置条目，
    // 以便最终提交上一个任期的日志条目。
    // 此外，该消息还可以帮助识别最后提交的索引和配置。
    CodeInjectionForTesting.execute(APPEND_PLACEHOLDER,
        server.getId().toString(), null);
    // Initialize startup log entry and append it to the RaftLog
    startupLogEntry.get();
    processor.start();
    senders.forEach(LogAppender::start);
  }



  boolean isReady() {
    return startupLogEntry.isInitialized()&& startupLogEntry.get().isApplied();
  }


  void checkReady(LogEntryProto entry) {
    if (entry.getTerm() == server.getState().getCurrentTerm() && startupLogEntry.get().checkStartIndex(entry)) {
      server.getStateMachine().leaderEvent().notifyLeaderReady();
    }
    //清除强制启动标志
    LeaderElection.cleanForceMode();
  }

  CompletableFuture<Void> stop() {
    if (!isStopped.compareAndSet(false, true)) {
      LOG.info("{} is already stopped", this);
      return CompletableFuture.completedFuture(null);
    }
    // do not interrupt event processor since it may be in the middle of logSync
    final CompletableFuture<Void> f = senders.stopAll();
    final NotLeaderException nle = server.generateNotLeaderException();
    final Collection<CommitInfoProto> commitInfos = server.getCommitInfos();
    try {
      final Collection<TransactionContext> transactions = pendingRequests.sendNotLeaderResponses(nle, commitInfos);
      server.getStateMachine().leaderEvent().notifyNotLeader(transactions);
      watchRequests.failWatches(nle);
    } catch (IOException e) {
      LOG.warn("{}: Caught exception in sendNotLeaderResponses", this, e);
    }
    messageStreamRequests.clear();
    readIndexHeartbeats.failListeners(nle);
    lease.getAndSetEnabled(false);
    startupLogEntry.get().getAppliedIndexFuture().completeExceptionally(
        new ReadIndexException("failed to obtain read index since: ", nle));
    server.getServerRpc().notifyNotLeader(server.getMemberId().getGroupId());
    logAppenderMetrics.unregister();
    raftServerMetrics.unregister();
    pendingRequests.close();
    watchRequests.close();
    return f;
  }

  void notifySenders() {
    senders.forEach(LogAppender::notifyLogAppender);
  }

  boolean inStagingState() {
    return stagingState != null;
  }

  long getCurrentTerm() {
    Preconditions.assertSame(currentTerm, server.getState().getCurrentTerm(), "currentTerm");
    return currentTerm;
  }

  @Override
  public boolean onFollowerTerm(FollowerInfo follower, long followerTerm) {
    if (isCaughtUp(follower) && followerTerm > getCurrentTerm()) {
      submitStepDownEvent(followerTerm, StepDownReason.HIGHER_TERM);
      return true;
    }
    return false;
  }

  /**
   * Start bootstrapping new peers
   */
  PendingRequest startSetConfiguration(SetConfigurationRequest request, List<RaftPeer> peersInNewConf) {
    LOG.info("{}: startSetConfiguration {}", this, request);
    Preconditions.assertTrue(isRunning(), () -> this + " is not running.");
    Preconditions.assertTrue(!inStagingState(), () -> this + " is already in staging state " + stagingState);

    final List<RaftPeer> listenersInNewConf = request.getArguments().getPeersInNewConf(RaftPeerRole.LISTENER);
    final Collection<RaftPeer> peersToBootStrap = server.getRaftConf().filterNotContainedInConf(peersInNewConf);
    final Collection<RaftPeer> listenersToBootStrap= server.getRaftConf().filterNotContainedInConf(listenersInNewConf);

    // add the request to the pending queue
    final PendingRequest pending = pendingRequests.addConfRequest(request);

    ConfigurationStagingState configurationStagingState = new ConfigurationStagingState(
        peersToBootStrap, listenersToBootStrap, new PeerConfiguration(peersInNewConf, listenersInNewConf));
    Collection<RaftPeer> newPeers = configurationStagingState.getNewPeers();
    Collection<RaftPeer> newListeners = configurationStagingState.getNewListeners();
    Collection<RaftPeer> allNew = newListeners.isEmpty()
        ? newPeers
        : newPeers.isEmpty()
            ? newListeners
            : Stream.concat(newPeers.stream(), newListeners.stream())
                .collect(Collectors.toList());

    if (allNew.isEmpty()) {
      applyOldNewConf(configurationStagingState);
    } else {
      // update the LeaderState's sender list
      Collection<LogAppender> newAppenders = addSenders(allNew);

      // set the staging state
      stagingState = configurationStagingState;

      newAppenders.forEach(LogAppender::start);
    }

    return pending;
  }

  PendingRequests.Permit tryAcquirePendingRequest(Message message) {
    return pendingRequests.tryAcquire(message);
  }

  PendingRequest addPendingRequest(PendingRequests.Permit permit, RaftClientRequest request, TransactionContext entry) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("{}: addPendingRequest at {}, entry={}", this, request,
          LogProtoUtils.toLogEntryString(entry.getLogEntryUnsafe()));
    }
    return pendingRequests.add(permit, request, entry);
  }

  CompletableFuture<RaftClientReply> streamAsync(ReferenceCountedObject<RaftClientRequest> requestRef) {
    RaftClientRequest request = requestRef.get();
    return messageStreamRequests.streamAsync(requestRef)
        .thenApply(dummy -> server.newSuccessReply(request))
        .exceptionally(e -> exception2RaftClientReply(request, e));
  }

  CompletableFuture<ReferenceCountedObject<RaftClientRequest>> streamEndOfRequestAsync(
      ReferenceCountedObject<RaftClientRequest> requestRef) {
    return messageStreamRequests.streamEndOfRequestAsync(requestRef);
  }

  CompletableFuture<RaftClientReply> addWatchRequest(RaftClientRequest request) {
    LOG.debug("{}: addWatchRequest {}", this, request);
    return watchRequests.add(request)
        .thenApply(logIndex -> server.newSuccessReply(request, logIndex))
        .exceptionally(e -> exception2RaftClientReply(request, e));
  }

  private RaftClientReply exception2RaftClientReply(RaftClientRequest request, Throwable e) {
    e = JavaUtils.unwrapCompletionException(e);
    if (e instanceof NotReplicatedException) {
      final NotReplicatedException nre = (NotReplicatedException)e;
      return server.newReplyBuilder(request)
          .setException(nre)
          .setLogIndex(nre.getLogIndex())
          .build();
    } else if (e instanceof NotLeaderException) {
      return server.newExceptionReply(request, (NotLeaderException)e);
    } else if (e instanceof LeaderNotReadyException) {
      return server.newExceptionReply(request, (LeaderNotReadyException)e);
    } else {
      throw new CompletionException(e);
    }
  }

  @Override
  public void onFollowerCommitIndex(FollowerInfo follower, long commitIndex) {
    if (follower.updateCommitIndex(commitIndex)) {
      commitIndexChanged();
    }
  }

  private void commitIndexChanged() {
    getMajorityMin(FollowerInfo::getCommitIndex, raftLog::getLastCommittedIndex).ifPresent(m -> {
      // Normally, leader commit index is always ahead of followers.
      // However, after a leader change, the new leader commit index may
      // be behind some followers in the beginning.
      watchRequests.update(ReplicationLevel.ALL_COMMITTED, m.min);
      watchRequests.update(ReplicationLevel.MAJORITY_COMMITTED, m.majority);
      watchRequests.update(ReplicationLevel.MAJORITY, m.max);
    });
    notifySenders();
  }

  private void applyOldNewConf(ConfigurationStagingState stage) {
    final ServerState state = server.getState();
    final RaftConfiguration current = state.getRaftConf();
    final long nextIndex = state.getLog().getNextIndex();
    final RaftConfiguration oldNewConf = stage.generateOldNewConf(current, nextIndex);
    // apply the (old, new) configuration to log, and use it as the current conf
    appendConfiguration(oldNewConf);

    notifySenders();
  }

  private long appendConfiguration(RaftConfiguration conf) {
    final long logIndex = raftLog.append(getCurrentTerm(), conf);
    Preconditions.assertSame(conf.getLogEntryIndex(), logIndex, "confLogIndex");
    server.getState().setRaftConf(conf);
    return logIndex;
  }

  void updateFollowerCommitInfos(CommitInfoCache cache, List<CommitInfoProto> protos) {
    for (LogAppender sender : senders) {
      FollowerInfo info = sender.getFollower();
      protos.add(cache.update(info.getPeer(), info.getCommitIndex()));
    }
  }

  @Override
  public AppendEntriesRequestProto newAppendEntriesRequestProto(FollowerInfo follower,
      List<LogEntryProto> entries, TermIndex previous, long callId) {
    final boolean initializing = !isCaughtUp(follower);
    final RaftPeerId targetId = follower.getId();
    String vnPeerId = getVnPeerId();
    return ServerProtoUtils.toAppendEntriesRequestProto(server.getMemberId(), targetId, getCurrentTerm(), entries,
        ServerImplUtils.effectiveCommitIndex(raftLog.getLastCommittedIndex(), previous, entries.size()),
        initializing, vnPeerId, previous, server.getCommitInfos(), callId);
  }

  /**
   * Update sender list for setConfiguration request
   */
  private void addAndStartSenders(Collection<RaftPeer> newPeers) {
    addSenders(newPeers).forEach(LogAppender::start);
  }

  private Collection<LogAppender> addSenders(Collection<RaftPeer> newPeers) {
    return !newPeers.isEmpty()
        ? addSenders(newPeers, RaftLog.LEAST_VALID_LOG_INDEX, false)
        : Collections.emptyList();
  }

  private RaftPeer getPeer(RaftPeerId id) {
    return server.getRaftConf().getPeer(id, RaftPeerRole.FOLLOWER, RaftPeerRole.LISTENER);
  }

  private LogAppender newLogAppender(FollowerInfo f) {
    return server.getRaftServer().getFactory().newLogAppender(server, this, f);
  }

  private Collection<LogAppender> addSenders(Collection<RaftPeer> newPeers, long nextIndex, boolean caughtUp) {
    final Timestamp t = Timestamp.currentTime().addTimeMs(-server.getMaxTimeoutMs());
    final List<LogAppender> newAppenders = new ArrayList<>();
    //添加物理节点跟随者
    final List<LogAppender> appenders = newPeers.stream()
      //.filter(e->!e.isVirtual())
      .map(peer -> {
      final FollowerInfo f = new FollowerInfoImpl(server.getMemberId(), peer, this::getPeer, t, nextIndex, caughtUp);
      followerInfoMap.put(peer.getId(), f);
      raftServerMetrics.addFollower(peer.getId());
      logAppenderMetrics.addFollowerGauges(peer.getId(), f::getNextIndex, f::getMatchIndex, f::getLastRpcTime);
      return newLogAppender(f);
    }).collect(Collectors.toList());
    newAppenders.addAll(appenders);
    senders.addAll(newAppenders);

    return newAppenders;
  }



  private void stopAndRemoveSenders(Predicate<LogAppender> predicate) {
    stopAndRemoveSenders(getLogAppenders().filter(predicate).collect(Collectors.toList()));
  }

  private void stopAndRemoveSenders(Collection<LogAppender> toStop) {
    toStop.forEach(LogAppender::stopAsync);
    senders.removeAll(toStop);
  }

  boolean isRunning() {
    if (isStopped.get()) {
      return false;
    }
    final LeaderStateImpl current = server.getRole().getLeaderState().orElse(null);
    return this == current;
  }

  @Override
  public void restart(LogAppender sender) {
    if (!isRunning()) {
      LOG.warn("Failed to restart {}: {} is not running", sender, this);
      return;
    }

    final FollowerInfo info = sender.getFollower();
    LOG.info("{}: Restarting {} for {}", this, JavaUtils.getClassSimpleName(sender.getClass()), info.getName());
    stopAndRemoveSenders(Collections.singleton(sender));

    Optional.ofNullable(getPeer(info.getId()))
        .ifPresent(peer -> addAndStartSenders(Collections.singleton(peer)));
  }

  /**
   * Update the RpcSender list based on the current configuration
   */
  private void updateSenders(RaftConfiguration conf) {
    Preconditions.assertTrue(conf.isStable() && !inStagingState());
    stopAndRemoveSenders(s -> !conf.containsInConf(s.getFollowerId(), RaftPeerRole.FOLLOWER, RaftPeerRole.LISTENER));
  }

  void submitStepDownEvent(StepDownReason reason) {
    submitStepDownEvent(currentTerm, reason);
  }

  void submitStepDownEvent(long term, StepDownReason reason) {
    eventQueue.submit(new StateUpdateEvent(StateUpdateEvent.Type.STEP_DOWN, term, () -> stepDown(term, reason)));
  }

  private void stepDown(long term, StepDownReason reason) {
    try {
      lease.getAndSetEnabled(false);
      server.changeToFollowerAndPersistMetadata(term, false, reason).join();
      pendingStepDown.complete(server::newSuccessReply);
    } catch(IOException e) {
      final String s = this + ": Failed to persist metadata for term " + term;
      LOG.warn(s, e);
      // the failure should happen while changing the state to follower
      // thus the in-memory state should have been updated
      if (!isStopped.get()) {
        throw new IllegalStateException(s + " and running == true", e);
      }
    }
  }

  CompletableFuture<RaftClientReply> submitStepDownRequestAsync(TransferLeadershipRequest request) {
    return pendingStepDown.submitAsync(request);
  }

  private static LogAppender chooseUpToDateFollower(List<LogAppender> followers, TermIndex leaderLastEntry) {
    for(LogAppender f : followers) {
      if (TransferLeadership.isFollowerUpToDate(f.getFollower(), leaderLastEntry)
          == TransferLeadership.Result.SUCCESS) {
        return f;
      }
    }
    return null;
  }

  private void prepare() {
    synchronized (server) {
      if (isRunning()) {
        final ServerState state = server.getState();
        if (state.getRaftConf().isTransitional() && state.isConfCommitted()) {
          // the configuration is in transitional state, and has been committed
          // so it is time to generate and replicate (new) conf.
          replicateNewConf();
        }
      }
    }
  }

  /**
   * The processor thread takes the responsibility to update the raft server's
   * state, such as changing to follower, or updating the committed index.
   */
  private class EventProcessor extends Daemon {
    public EventProcessor(String name, RaftServerImpl server) {
      super(Daemon.newBuilder()
          .setName(name).setThreadGroup(server.getThreadGroup()));
    }
    @Override
    public void run() {
      // apply an empty message; check if necessary to replicate (new) conf
      prepare();

      while (isRunning()) {
        final StateUpdateEvent event = eventQueue.poll();
        synchronized(server) {
          if (isRunning()) {
            if (event != null) {
              event.execute();
            } else if (inStagingState()) {
              checkStaging();
            } else if (checkLeadership()) {
              checkPeersForYieldingLeader();
            }
          }
        }
      }
    }
  }

  /**
   * So far we use a simple implementation for catchup checking:
   * 1. If the latest rpc time of the remote peer is before 3 * max_timeout,
   *    the peer made no progress for that long. We should fail the whole
   *    setConfiguration request.
   * 2. If the peer's matching index is just behind for a small gap, and the
   *    peer was updated recently (within max_timeout), declare the peer as
   *    caught-up.
   * 3. Otherwise the peer is making progressing. Keep waiting.
   */
  private BootStrapProgress checkProgress(FollowerInfo follower, long committed) {
    Preconditions.assertTrue(!isCaughtUp(follower));
    final Timestamp progressTime = Timestamp.currentTime().addTimeMs(-server.getMaxTimeoutMs());
    final Timestamp timeoutTime = Timestamp.currentTime().addTimeMs(-3L * server.getMaxTimeoutMs());
    if (follower.getLastRpcResponseTime().compareTo(timeoutTime) < 0) {
      LOG.debug("{} detects a follower {} timeout ({}ms) for bootstrapping", this, follower,
          follower.getLastRpcResponseTime().elapsedTimeMs());
      return BootStrapProgress.NOPROGRESS;
    } else if (follower.getMatchIndex() + stagingCatchupGap > committed
        && follower.getLastRpcResponseTime().compareTo(progressTime) > 0
        && follower.hasAttemptedToInstallSnapshot()) {
      return BootStrapProgress.CAUGHTUP;
    } else {
      return BootStrapProgress.PROGRESSING;
    }
  }

  @Override
  public void onFollowerSuccessAppendEntries(FollowerInfo follower) {
    if (isCaughtUp(follower)) {
      submitUpdateCommitEvent();
    } else {
      eventQueue.submit(checkStagingEvent);
    }
    server.getTransferLeadership().onFollowerAppendEntriesReply(follower);
  }

  @Override
  public boolean isFollowerBootstrapping(FollowerInfo follower) {
    return isBootStrappingPeer(follower.getId());
  }

  private void checkStaging() {
    if (!inStagingState()) {
      // it is possible that the bootstrapping is done. Then, fallback to UPDATE_COMMIT
      updateCommitEvent.execute();
    } else {
      final long commitIndex = server.getState().getLog().getLastCommittedIndex();
      // check progress for the new followers
      final List<FollowerInfo> laggingFollowers = getLogAppenders()
          .map(LogAppender::getFollower)
          .filter(follower -> !isCaughtUp(follower))
          //.map(FollowerInfoImpl.class::cast)
          .collect(Collectors.toList());
      final EnumSet<BootStrapProgress> reports = laggingFollowers.stream()
          .map(follower -> checkProgress(follower, commitIndex))
          .collect(Collectors.toCollection(() -> EnumSet.noneOf(BootStrapProgress.class)));
      if (reports.contains(BootStrapProgress.NOPROGRESS)) {
        stagingState.fail(BootStrapProgress.NOPROGRESS);
      } else if (!reports.contains(BootStrapProgress.PROGRESSING)) {
        // all caught up!
        applyOldNewConf(stagingState);
        this.stagingState = null;
        laggingFollowers.stream()
            .filter(f -> server.getRaftConf().containsInConf(f.getId()))
            .forEach(FollowerInfo::catchUp);
      }
    }
  }

  boolean isBootStrappingPeer(RaftPeerId peerId) {
    return Optional.ofNullable(stagingState).map(s -> s.contains(peerId)).orElse(false);
  }

  void submitUpdateCommitEvent() {
    eventQueue.submit(updateCommitEvent);
  }

  static class MinMajorityMax {
    private final long min;
    private final long majority;
    private final long max;

    MinMajorityMax(long min, long majority, long max) {
      this.min = min;
      this.majority = majority;
      this.max = max;
    }

    MinMajorityMax combine(MinMajorityMax that) {
      return new MinMajorityMax(
          Math.min(this.min, that.min),
          Math.min(this.majority, that.majority),
          Math.min(this.max, that.max));
    }

    static MinMajorityMax valueOf(long[] sorted) {
      return new MinMajorityMax(sorted[0], getMajority(sorted), getMax(sorted));
    }

    static MinMajorityMax valueOf(long[] sorted, long gapThreshold) {
      long majority = getMajority(sorted);
      long min = sorted[0];
      if (gapThreshold != -1 && (majority - min) > gapThreshold) {
        // The the gap between majority and min(the slow follower) is greater than gapThreshold,
        // set the majority to min, which will skip one round of lastCommittedIndex update in updateCommit().
        majority = min;
      }
      return new MinMajorityMax(min, majority, getMax(sorted));
    }

    static long getMajority(long[] sorted) {
      return sorted[(sorted.length - 1) / 2];
    }

    static long getMax(long[] sorted) {
      return sorted[sorted.length - 1];
    }
  }

  private void updateCommit() {
    //获取已写入有效份数的最小索引，修改该索引为已提交
    getMajorityMin(FollowerInfo::getMatchIndex, raftLog::getFlushIndex,
        followerMaxGapThreshold)
    .ifPresent(m -> updateCommit(m.majority, m.min));
  }

  private Optional<MinMajorityMax> getMajorityMin(ToLongFunction<FollowerInfo> followerIndex, LongSupplier logIndex) {
    return getMajorityMin(followerIndex, logIndex, -1);
  }

  private Optional<MinMajorityMax> getMajorityMin(ToLongFunction<FollowerInfo> followerIndex,
      LongSupplier logIndex, long gapThreshold) {
    final RaftPeerId selfId = server.getId();
    final RaftConfiguration conf = server.getRaftConf();

    final CurrentOldFollowerInfos infos = followerInfoMap.getFollowerInfos(conf);
    final List<FollowerInfo> followers = infos.getCurrent();
    final boolean includeSelf = conf.containsInConf(selfId);
    if (followers.isEmpty() && !includeSelf) {
      return Optional.empty();
    }
    //获取有效索引的排序数组
    final long[] indicesInNewConf = getSorted(followers, includeSelf, followerIndex, logIndex);

    final MinMajorityMax newConf = MinMajorityMax.valueOf(indicesInNewConf, gapThreshold);

    if (!conf.isTransitional()) {
      return Optional.of(newConf);
    } else { // configuration is in transitional state
      final List<FollowerInfo> oldFollowers = infos.getOld();
      final boolean includeSelfInOldConf = conf.containsInOldConf(selfId);
      if (oldFollowers.isEmpty() && !includeSelfInOldConf) {
        return Optional.empty();
      }

      final long[] indicesInOldConf = getSorted(oldFollowers, includeSelfInOldConf, followerIndex, logIndex);
      final MinMajorityMax oldConf = MinMajorityMax.valueOf(indicesInOldConf, gapThreshold);
      return Optional.of(newConf.combine(oldConf));
    }
  }

  private boolean hasMajority(Predicate<RaftPeerId> isAcked) {
    final RaftPeerId selfId = server.getId();
    return server.getRaftConf().hasMajority(isAcked, selfId);
  }

  private void updateCommit(LogEntryHeader[] entriesToCommit) {
    final long newCommitIndex = raftLog.getLastCommittedIndex();
    long lastCommitIndex = RaftLog.INVALID_LOG_INDEX;

    boolean hasConfiguration = false;
    for (LogEntryHeader entry : entriesToCommit) {
      if (entry.getIndex() > newCommitIndex) {
        break;
      }
      hasConfiguration |= entry.getLogEntryBodyCase() == LogEntryBodyCase.CONFIGURATIONENTRY;
      raftLog.getRaftLogMetrics().onLogEntryCommitted(entry);
      if (entry.getLogEntryBodyCase() != LogEntryBodyCase.METADATAENTRY) {
        lastCommitIndex = entry.getIndex();
      }
    }
    if (logMetadataEnabled && lastCommitIndex != RaftLog.INVALID_LOG_INDEX) {
      logMetadata(lastCommitIndex);
    }
    commitIndexChanged();
    if (hasConfiguration) {
      checkAndUpdateConfiguration();
    }
  }

  private void updateCommit(long majority, long min) {
    final long oldLastCommitted = raftLog.getLastCommittedIndex();
		if (majority > oldLastCommitted) {
      // Get the headers before updating commit index since the log can be purged after a snapshot
      final LogEntryHeader[] entriesToCommit = raftLog.getEntries(oldLastCommitted + 1, majority + 1);

      if (server.getState().updateCommitIndex(majority, currentTerm, true)) {
        updateCommit(entriesToCommit);
      }
    }
    watchRequests.update(ReplicationLevel.ALL, min);
  }

  private void logMetadata(long commitIndex) {
    if (raftLog.appendMetadata(currentTerm, commitIndex) != RaftLog.INVALID_LOG_INDEX) {
      notifySenders();
    }
  }

  private void checkAndUpdateConfiguration() {
    final RaftConfiguration conf = server.getRaftConf();
    if (conf.isTransitional()) {
      replicateNewConf();
    } else { // the (new) log entry has been committed
      pendingRequests.replySetConfiguration(server::newSuccessReply);
      // if the leader is not included in the current configuration, step down
      if (!conf.containsInConf(server.getId(), RaftPeerRole.FOLLOWER, RaftPeerRole.LISTENER)) {
        lease.getAndSetEnabled(false);
        LOG.info("{} is not included in the new configuration {}. Will shutdown server...", this, conf);
        try {
          // leave some time for all RPC senders to send out new conf entry
          server.properties().minRpcTimeout().sleep();
        } catch (InterruptedException ignored) {
          Thread.currentThread().interrupt();
        }
        // the pending request handler will send NotLeaderException for
        // pending client requests when it stops
        server.close();
      }
    }
  }

  /**
   * when the (old, new) log entry has been committed, should replicate (new):
   * 1) append (new) to log
   * 2) update conf to (new)
   * 3) update RpcSenders list
   * 4) start replicating the log entry
   */
  private void replicateNewConf() {
    final RaftConfiguration conf = server.getRaftConf();
    final RaftConfiguration newConf = RaftConfigurationImpl.newBuilder()
        .setConf(conf)
        .setLogEntryIndex(raftLog.getNextIndex())
        .build();
    // stop the LogAppender if the corresponding follower and listener is no longer in the conf
    updateSenders(newConf);
    appendConfiguration(newConf);
    notifySenders();
  }

  private long[] getSorted(List<FollowerInfo> followerInfos, boolean includeSelf,
      ToLongFunction<FollowerInfo> getFollowerIndex, LongSupplier getLogIndex) {
    List<FollowerInfo> validFollowerInfos = getValidFollowerInfos(followerInfos);
    final int length = includeSelf ? validFollowerInfos.size() + 1 : validFollowerInfos.size();
    if (length == 0) {
      throw new IllegalArgumentException("followerInfos is empty and includeSelf == " + includeSelf);
    }

    final long[] indices = new long[length];
    for (int i = 0; i < validFollowerInfos.size(); i++) {
      indices[i] = getFollowerIndex.applyAsLong(validFollowerInfos.get(i));
    }

    if (includeSelf) {
      // note that we also need to wait for the local disk I/O
      indices[length - 1] = getLogIndex.getAsLong();
    }

    Arrays.sort(indices);
    return indices;
  }

  /**
   * 获取有效追随者，虚拟节点只有有一个。
   * Leader不是虚拟节点则添加一个有效的虚拟节点
   */
  private  List<FollowerInfo> getValidFollowerInfos(List<FollowerInfo> followerInfos) {
    List<FollowerInfo> validFollowerInfos = new ArrayList<>();
    followerInfos.stream().filter(e->!e.getId().isVirtual())
      .forEach(validFollowerInfos::add);
    // Leader不是虚拟节点则添加一个有效的虚拟节点
    if(!leaderId.isVirtual()) {
      followerInfos.stream().filter(e -> e.getId().isVirtual())
          .max(Comparator.comparingLong(FollowerInfo::getMatchIndex))
          .ifPresent(validFollowerInfos::add);
    }
    return validFollowerInfos;
  }

  private void checkPeersForYieldingLeader() {
    final RaftConfiguration conf = server.getRaftConf();
    final RaftPeer leader = conf.getPeer(server.getId());
    if (leader == null) {
      LOG.error("{} the leader {} is not in the conf {}", this, server.getId(), conf);
      return;
    }
    final int leaderPriority = leader.getPriority();

    final List<LogAppender> highestPriorityInfos = new ArrayList<>();
    int highestPriority = Integer.MIN_VALUE;
    for (LogAppender logAppender : senders) {
      final RaftPeer follower = conf.getPeer(logAppender.getFollowerId());
      if (follower == null) {
        continue;
      }
      final int followerPriority = follower.getPriority();
      if (followerPriority > leaderPriority && followerPriority >= highestPriority) {
        if (followerPriority > highestPriority) {
          highestPriority = followerPriority;
          highestPriorityInfos.clear();
        }
        highestPriorityInfos.add(logAppender);
      }
    }
    final TermIndex leaderLastEntry = server.getState().getLastEntry();
    final LogAppender appender = chooseUpToDateFollower(highestPriorityInfos, leaderLastEntry);
    if (appender != null) {
      server.getTransferLeadership().start(appender);
    }
  }

  /**
   * See the thesis section 6.2: A leader in Raft steps down
   * if an election timeout elapses without a successful
   * round of heartbeats to a majority of its cluster.
   */

  public boolean checkLeadership() {

    if (!server.getRole().getLeaderState().filter(leader -> leader == this).isPresent()) {
      return false;
    }
    // The initial value of lastRpcResponseTime in FollowerInfo is set by
    // LeaderState::addSenders(), which is fake and used to trigger an
    // immediate round of AppendEntries request. Since candidates collect
    // votes from majority before becoming leader, without seeing higher term,
    // ideally, A leader is legal for election timeout if become leader soon.
    if (server.getRole().getRoleElapsedTimeMs() < server.getMaxTimeoutMs()) {
      return true;
    }

    final List<RaftPeerId> activePeers = getLogAppenders()
        .filter(sender -> sender.getFollower()
                                .getLastRpcResponseTime()
                                .elapsedTimeMs() <= server.getMaxTimeoutMs())
        .map(LogAppender::getFollowerId)
        .collect(Collectors.toList());
    final RaftConfiguration conf = server.getRaftConf();
    if (conf.hasMajority(activePeers, server.getId())) {
      // leadership check passed
      return true;
    }

		LOG.warn("{}: Lost leadership on term: {}. Election timeout: {}ms. In charge for: {}ms. Conf: {}",
      this, currentTerm, server.getMaxTimeoutMs(), server.getRole().getRoleElapsedTimeMs(), conf);
    getLogAppenders().map(LogAppender::getFollower).forEach(f -> LOG.warn("Follower {}", f));

    // step down as follower
    stepDown(currentTerm, StepDownReason.LOST_MAJORITY_HEARTBEATS);
    return false;
  }

  /**
   * Obtain the current readIndex for read only requests. See Raft paper section 6.4.
   * 1. Leader makes sure at least one log from current term is committed.
   * 2. Leader record last committed index as readIndex.
   * 3. Leader broadcast heartbeats to followers and waits for acknowledgements.
   * 4. If majority respond success, returns readIndex.
   * @return current readIndex.
   */
  CompletableFuture<Long> getReadIndex(Long readAfterWriteConsistentIndex) {
    final long readIndex;
    if (readAfterWriteConsistentIndex != null) {
      readIndex = readAfterWriteConsistentIndex;
    } else {
      readIndex = server.getRaftLog().getLastCommittedIndex();
    }
    LOG.debug("readIndex={}, readAfterWriteConsistentIndex={}", readIndex, readAfterWriteConsistentIndex);

    // if group contains only one member, fast path
    if (server.getRaftConf().isSingleton()) {
      return CompletableFuture.completedFuture(readIndex);
    }

    // leader has not committed any entries in this term
    if (!isReady()) {
      return startupLogEntry.get().getAppliedIndexFuture();
    }

    // if lease is enabled, check lease first
    if (hasLease()) {
      return CompletableFuture.completedFuture(readIndex);
    }

    // send heartbeats and wait for majority acknowledgments
    final AppendEntriesListener listener = readIndexHeartbeats.addAppendEntriesListener(
        readIndex, i -> new AppendEntriesListener(i, senders));

    // the readIndex is already acknowledged before
    if (listener == null) {
      return CompletableFuture.completedFuture(readIndex);
    }

    return listener.getFuture();
  }

  @Override
  public void onAppendEntriesReply(LogAppender appender, AppendEntriesReplyProto reply) {
    readIndexHeartbeats.onAppendEntriesReply(appender, reply, this::hasMajority);
  }

  boolean getAndSetLeaseEnabled(boolean newValue) {
    return lease.getAndSetEnabled(newValue);
  }

  boolean hasLease() {
    if (!lease.isEnabled()) {
      return false;
    }

    if (checkLeaderLease()) {
      return true;
    }

    // try extending the leader lease
    final RaftConfiguration conf = server.getRaftConf();
    final CurrentOldFollowerInfos info = followerInfoMap.getFollowerInfos(conf);
    lease.extend(info.getCurrent(), info.getOld(), peers -> conf.hasMajority(peers, server.getId()));

    return checkLeaderLease();
  }

  private boolean checkLeaderLease() {
    return isRunning() && isReady()
        && (server.getRaftConf().isSingleton() || lease.isValid());
  }

  void replyPendingRequest(TermIndex termIndex, RaftClientReply reply) {
    pendingRequests.replyPendingRequest(termIndex, reply);
  }

  TransactionContext getTransactionContext(TermIndex termIndex) {
    return pendingRequests.getTransactionContext(termIndex);
  }

  long[] getFollowerNextIndices() {
    return getLogAppenders().mapToLong(s -> s.getFollower().getNextIndex()).toArray();
  }

  static Map<RaftPeerId, RaftPeer> newMap(Collection<RaftPeer> peers, String str) {
    Objects.requireNonNull(peers, () -> str + " == null");
    final Map<RaftPeerId, RaftPeer> map = new HashMap<>();
    for(RaftPeer p : peers) {
      map.put(p.getId(), p);
    }
    return Collections.unmodifiableMap(map);
  }

  private class ConfigurationStagingState {
    private final String name = server.getMemberId() + "-" + JavaUtils.getClassSimpleName(getClass());
    private final Map<RaftPeerId, RaftPeer> newPeers;
    private final Map<RaftPeerId, RaftPeer> newListeners;
    private final PeerConfiguration newConf;

    ConfigurationStagingState(Collection<RaftPeer> newPeers, Collection<RaftPeer> newListeners,
        PeerConfiguration newConf) {
      this.newPeers = newMap(newPeers, "peer");
      this.newListeners = newMap(newListeners, "listeners");
      this.newConf = newConf;
    }

    RaftConfiguration generateOldNewConf(RaftConfiguration current, long logIndex) {
      return RaftConfigurationImpl.newBuilder()
          .setConf(newConf)
          .setOldConf(current)
          .setLogEntryIndex(logIndex)
          .build();
    }

    Collection<RaftPeer> getNewPeers() {
      return newPeers.values();
    }

    Collection<RaftPeer> getNewListeners() {
      return newListeners.values();
    }

    boolean contains(RaftPeerId peerId) {
      return newPeers.containsKey(peerId) || newListeners.containsKey(peerId);
    }

    void fail(BootStrapProgress progress) {
      final String message = this + ": Fail to set configuration " + newConf + " due to " + progress;
      LOG.debug(message);
      stopAndRemoveSenders(s -> !isCaughtUp(s.getFollower()));

      stagingState = null;
      // send back failure response to client's request
      pendingRequests.failSetConfiguration(new ReconfigurationTimeoutException(message));
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /**
   * @return the RaftPeer (address and id) information of the followers.
   */
  Stream<RaftPeer> getFollowers() {
    return getLogAppenders()
        .map(sender -> sender.getFollower().getPeer())
        .filter(peer -> server.getRaftConf().containsInConf(peer.getId()));
  }

  Stream<LogAppender> getLogAppenders() {
    return StreamSupport.stream(senders.spliterator(), false);
  }

  Optional<LogAppender> getLogAppender(RaftPeerId id) {
    return getLogAppenders().filter(a -> a.getFollowerId().equals(id)).findAny();
  }

  private static boolean isCaughtUp(FollowerInfo follower) {
    return follower.isCaughtUp();
  }

  @Override
  public void checkHealth(FollowerInfo follower) {
    final TimeDuration elapsedTime = follower.getLastRpcResponseTime().elapsedTime();
    if (elapsedTime.compareTo(server.properties().rpcSlownessTimeout()) > 0) {
      compareAndSetVnPeerId(follower.getPeer().getId().getId(), null);
      final RoleInfoProto leaderInfo = server.getInfo().getRoleInfoProto();
      server.getStateMachine().leaderEvent().notifyFollowerSlowness(leaderInfo, follower.getPeer());
    }
    final RaftPeerId followerId = follower.getId();
    raftServerMetrics.recordFollowerHeartbeatElapsedTime(followerId, elapsedTime.toLong(TimeUnit.NANOSECONDS));
  }

  @Override
  public String getVnPeerId(){
    if(leaderId.isVirtual()){
      return leaderId.getId();
    }
    return Optional.ofNullable(vnPeerId.get()).orElse("");
  }

  @Override
  public boolean compareAndSetVnPeerId(String expect, String update) {
    if(leaderId.isVirtual()){
      return false;
    }
    boolean b = vnPeerId.compareAndSet(expect, update);
    if (b) {
      LOG.info("compareAndSetVnPeerId: expect={}, update={}", expect, update);
    }
    return b;
  }

  @Override
  public String toString() {
    return name;
  }
}
