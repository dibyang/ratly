package net.xdob.ratly.server.impl;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.*;

import net.xdob.ratly.client.impl.ClientProtoUtils;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.metrics.Timekeeper;
import net.xdob.ratly.proto.raft.AppendEntriesReplyProto;
import net.xdob.ratly.proto.raft.AppendEntriesReplyProto.AppendResult;
import net.xdob.ratly.proto.raft.AppendEntriesRequestProto;
import net.xdob.ratly.proto.raft.CommitInfoProto;
import net.xdob.ratly.proto.raft.LogInfoProto;
import net.xdob.ratly.proto.raft.InstallSnapshotReplyProto;
import net.xdob.ratly.proto.raft.InstallSnapshotRequestProto;
import net.xdob.ratly.proto.raft.InstallSnapshotResult;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.proto.raft.RaftClientRequestProto;
import net.xdob.ratly.proto.raft.RaftClientRequestProto.TypeCase;
import net.xdob.ratly.proto.raft.RaftConfigurationProto;
import net.xdob.ratly.proto.raft.RaftPeerRole;
import net.xdob.ratly.proto.raft.RaftRpcRequestProto;
import net.xdob.ratly.proto.raft.ReadIndexReplyProto;
import net.xdob.ratly.proto.raft.ReadIndexRequestProto;
import net.xdob.ratly.proto.raft.ReplicationLevel;
import net.xdob.ratly.proto.raft.RequestVoteReplyProto;
import net.xdob.ratly.proto.raft.RequestVoteRequestProto;
import net.xdob.ratly.proto.raft.RoleInfoProto;
import net.xdob.ratly.proto.raft.StartLeaderElectionReplyProto;
import net.xdob.ratly.proto.raft.StartLeaderElectionRequestProto;
import net.xdob.ratly.protocol.*;
import net.xdob.ratly.protocol.exceptions.GroupMismatchException;
import net.xdob.ratly.protocol.exceptions.LeaderNotReadyException;
import net.xdob.ratly.protocol.exceptions.LeaderSteppingDownException;
import net.xdob.ratly.protocol.exceptions.NotLeaderException;
import net.xdob.ratly.protocol.exceptions.RaftException;
import net.xdob.ratly.protocol.exceptions.ReadException;
import net.xdob.ratly.protocol.exceptions.ReadIndexException;
import net.xdob.ratly.protocol.exceptions.ReconfigurationInProgressException;
import net.xdob.ratly.protocol.exceptions.ResourceUnavailableException;
import net.xdob.ratly.protocol.exceptions.ServerNotReadyException;
import net.xdob.ratly.protocol.exceptions.SetConfigurationException;
import net.xdob.ratly.protocol.exceptions.StaleReadException;
import net.xdob.ratly.protocol.exceptions.StateMachineException;
import net.xdob.ratly.protocol.exceptions.TransferLeadershipException;
import net.xdob.ratly.server.*;
import net.xdob.ratly.server.config.*;
import net.xdob.ratly.server.impl.LeaderElection.Phase;
import net.xdob.ratly.server.impl.RetryCacheImpl.CacheEntry;
import net.xdob.ratly.server.leader.LeaderState;
import net.xdob.ratly.server.metrics.LeaderElectionMetrics;
import net.xdob.ratly.server.metrics.RaftServerMetricsImpl;
import net.xdob.ratly.server.protocol.RaftServerAsynchronousProtocol;
import net.xdob.ratly.server.protocol.RaftServerProtocol;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.LogProtoUtils;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.raftlog.RaftLogIOException;
import net.xdob.ratly.server.storage.RaftStorage;
import net.xdob.ratly.server.storage.RaftStorageDirectory;
import net.xdob.ratly.server.storage.StartupOption;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.statemachine.StateMachine;
import net.xdob.ratly.statemachine.TransactionContext;
import net.xdob.ratly.statemachine.impl.TransactionContextImpl;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import net.xdob.ratly.util.CodeInjectionForTesting;
import net.xdob.ratly.util.Collections3;
import net.xdob.ratly.util.Concurrents3;
import net.xdob.ratly.util.FileUtils;
import net.xdob.ratly.util.IOUtils;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.LifeCycle;
import net.xdob.ratly.util.LifeCycle.State;
import net.xdob.ratly.util.MemoizedSupplier;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.ProtoUtils;
import net.xdob.ratly.util.ReferenceCountedObject;
import net.xdob.ratly.util.TimeDuration;
import net.xdob.ratly.util.function.CheckedSupplier;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static net.xdob.ratly.server.impl.ServerImplUtils.assertEntries;
import static net.xdob.ratly.server.impl.ServerImplUtils.assertGroup;
import static net.xdob.ratly.server.impl.ServerImplUtils.effectiveCommitIndex;
import static net.xdob.ratly.server.impl.ServerProtoUtils.toAppendEntriesReplyProto;
import static net.xdob.ratly.server.impl.ServerProtoUtils.toReadIndexReplyProto;
import static net.xdob.ratly.server.impl.ServerProtoUtils.toReadIndexRequestProto;
import static net.xdob.ratly.server.impl.ServerProtoUtils.toRequestVoteReplyProto;
import static net.xdob.ratly.server.impl.ServerProtoUtils.toStartLeaderElectionReplyProto;
import static net.xdob.ratly.server.util.ServerStringUtils.toAppendEntriesReplyString;
import static net.xdob.ratly.server.util.ServerStringUtils.toAppendEntriesRequestString;
import static net.xdob.ratly.server.util.ServerStringUtils.toRequestVoteReplyString;

class RaftServerImpl implements Division,
    RaftServerProtocol, RaftServerAsynchronousProtocol,
    RaftClientProtocol, RaftClientAsynchronousProtocol {
  private static final String CLASS_NAME = JavaUtils.getClassSimpleName(RaftServerImpl.class);
  static final String REQUEST_VOTE = CLASS_NAME + ".requestVote";
  static final String APPEND_ENTRIES = CLASS_NAME + ".appendEntries";
  static final String INSTALL_SNAPSHOT = CLASS_NAME + ".installSnapshot";
  static final String APPEND_TRANSACTION = CLASS_NAME + ".appendTransaction";
  static final String LOG_SYNC = APPEND_ENTRIES + ".logComplete";
  static final String START_LEADER_ELECTION = CLASS_NAME + ".startLeaderElection";
  static final String START_COMPLETE = CLASS_NAME + ".startComplete";
  private final ScheduledExecutorService scheduled;


  class Info implements DivisionInfo {
    @Override
    public RaftPeerRole getCurrentRole() {
      return getRole().getCurrentRole();
    }

    @Override
    public boolean isLeaderReady() {
      return isLeader() && getRole().isLeaderReady();
    }

    @Override
    public RaftPeerId getLeaderId() {
      return getState().getLeaderId();
    }

    @Override
    public LifeCycle.State getLifeCycleState() {
      return lifeCycle.getCurrentState();
    }

    @Override
    public RoleInfoProto getRoleInfoProto() {
      return RaftServerImpl.this.getRoleInfoProto();
    }

    @Override
    public long getCurrentTerm() {
      return getState().getCurrentTerm();
    }

    @Override
    public long getLastAppliedIndex() {
      return getState().getLastAppliedIndex();
    }

    @Override
    public long[] getFollowerNextIndices() {
      return role.getLeaderState()
          .filter(leader -> isLeader())
          .map(LeaderStateImpl::getFollowerNextIndices)
          .orElse(null);
    }
  }

  private final RaftServerProxy proxy;
  private final StateMachine stateMachine;
  private final Info info =  new Info();

  private final DivisionProperties divisionProperties;
  private final TimeDuration leaderStepDownWaitTime;
  private final boolean memberMajorityAddEnabled;
  private final TimeDuration sleepDeviationThreshold;

  private final LifeCycle lifeCycle;
  private ServerState state;
  private final RoleInfo role;

  private final DataStreamMap dataStreamMap;
  private final RaftServerConfigKeys.Read.Option readOption;

  private final TransactionManager transactionManager;
  private final RetryCacheImpl retryCache;
  private final CommitInfoCache commitInfoCache = new CommitInfoCache();
  private final WriteIndexCache writeIndexCache;

  private final RaftServerJmxAdapter jmxAdapter = new RaftServerJmxAdapter(this);
  private final LeaderElectionMetrics leaderElectionMetrics;
  private final RaftServerMetricsImpl raftServerMetrics;
  private final CountDownLatch closeFinishedLatch = new CountDownLatch(1);

  // To avoid append entry before complete start() method
  // For example, if thread1 start(), but before thread1 startAsFollower(), thread2 receive append entry
  // request, and change state to RUNNING by lifeCycle.compareAndTransition(STARTING, RUNNING),
  // then thread1 execute lifeCycle.transition(RUNNING) in startAsFollower(),
  // So happens IllegalStateException: ILLEGAL TRANSITION: RUNNING -> RUNNING,
  private final AtomicBoolean startComplete;

  private final TransferLeadership transferLeadership;
  private final SnapshotManagementRequestHandler snapshotRequestHandler;
  private final SnapshotInstallationHandler snapshotInstallationHandler;

  private final ExecutorService serverExecutor;
  private final ExecutorService clientExecutor;

  private final AtomicBoolean firstElectionSinceStartup = new AtomicBoolean(true);
  private final ThreadGroup threadGroup;

  private final AtomicBoolean stateStart = new AtomicBoolean(false);
  private final VNodeLease vnodeLease;
  private final String storageMount;

  RaftServerImpl(RaftGroup group, StateMachine stateMachine, RaftServerProxy proxy, StartupOption option)
      throws IOException {
    final RaftPeerId id = proxy.getId();
    LOG.info("{}: new RaftServerImpl for {} with {}", id, group, stateMachine);
    this.lifeCycle = new LifeCycle(id);
    this.stateMachine = stateMachine;
    this.role = new RoleInfo(id);

    final RaftProperties properties = proxy.getProperties();
    this.divisionProperties = new DivisionPropertiesImpl(properties);
    this.leaderStepDownWaitTime = RaftServerConfigKeys.LeaderElection.leaderStepDownWaitTime(properties);
    this.memberMajorityAddEnabled = RaftServerConfigKeys.LeaderElection.memberMajorityAdd(properties);
    this.sleepDeviationThreshold = RaftServerConfigKeys.sleepDeviationThreshold(properties);
    this.proxy = proxy;
    this.state = new ServerState(id, group, stateMachine, this, option, properties);
    this.retryCache = new RetryCacheImpl(properties);
    this.dataStreamMap = new DataStreamMapImpl(id);
    this.readOption = RaftServerConfigKeys.Read.option(properties);
    this.writeIndexCache = new WriteIndexCache(properties);
    this.transactionManager = new TransactionManager(id);

    this.leaderElectionMetrics = LeaderElectionMetrics.getLeaderElectionMetrics(
        getMemberId(), state::getLastLeaderElapsedTimeMs);
    this.raftServerMetrics = RaftServerMetricsImpl.computeIfAbsentRaftServerMetrics(
        getMemberId(), this::getCommitIndex, retryCache::getStatistics);

    this.startComplete = new AtomicBoolean(false);
    this.threadGroup = new ThreadGroup(proxy.getThreadGroup(), getMemberId().toString());

    this.transferLeadership = new TransferLeadership(this, properties);
    this.snapshotRequestHandler = new SnapshotManagementRequestHandler(this);
    this.snapshotInstallationHandler = new SnapshotInstallationHandler(this, properties);

    this.serverExecutor = Concurrents3.newThreadPoolWithMax(
        RaftServerConfigKeys.ThreadPool.serverCached(properties),
        RaftServerConfigKeys.ThreadPool.serverSize(properties),
        id + "-server");
    this.clientExecutor = Concurrents3.newThreadPoolWithMax(
        RaftServerConfigKeys.ThreadPool.clientCached(properties),
        RaftServerConfigKeys.ThreadPool.clientSize(properties),
        id + "-client");

    this.storageMount = RaftServerConfigKeys.storageMount(properties);

    this.vnodeLease = new VNodeLease(properties);
    this.scheduled = Executors.newScheduledThreadPool(1);
  }

  private long getCommitIndex(RaftPeerId id) {
    return commitInfoCache.get(id).orElse(0L);
  }

  @Override
  public DivisionProperties properties() {
    return divisionProperties;
  }

  int getMaxTimeoutMs() {
    return properties().maxRpcTimeoutMs();
  }

  TimeDuration getRandomElectionTimeout() {
    if (firstElectionSinceStartup.get()) {
      return getFirstRandomElectionTimeout();
    }
    final int min = properties().minRpcTimeoutMs();
    final int millis = min + ThreadLocalRandom.current().nextInt(properties().maxRpcTimeoutMs() - min + 1);
    return TimeDuration.valueOf(millis, TimeUnit.MILLISECONDS);
  }

  private TimeDuration getFirstRandomElectionTimeout() {
    final RaftProperties properties = proxy.getProperties();
    final int min = RaftServerConfigKeys.Rpc.firstElectionTimeoutMin(properties).toIntExact(TimeUnit.MILLISECONDS);
    final int max = RaftServerConfigKeys.Rpc.firstElectionTimeoutMax(properties).toIntExact(TimeUnit.MILLISECONDS);
    final int mills = min + ThreadLocalRandom.current().nextInt(max - min + 1);
    return TimeDuration.valueOf(mills, TimeUnit.MILLISECONDS);
  }

  TimeDuration getLeaderStepDownWaitTime() {
    return leaderStepDownWaitTime;
  }

  TimeDuration getSleepDeviationThreshold() {
    return sleepDeviationThreshold;
  }

  @Override
  public ThreadGroup getThreadGroup() {
    return threadGroup;
  }

  @Override
  public StateMachine getStateMachine() {
    return stateMachine;
  }

  @Override
  public RaftLog getRaftLog() {
    return getState().getLog();
  }

  @Override
  public RaftStorage getRaftStorage() {
    return getState().getStorage();
  }

  @Override
  public DataStreamMap getDataStreamMap() {
    return dataStreamMap;
  }

  @Override
  public RetryCacheImpl getRetryCache() {
    return retryCache;
  }

  @Override
  public RaftServerProxy getRaftServer() {
    return proxy;
  }

  TransferLeadership getTransferLeadership() {
    return transferLeadership;
  }

  RaftServerRpc getServerRpc() {
    return proxy.getServerRpc();
  }

  private void setRole(RaftPeerRole newRole, Object reason) {
    LOG.info("{}: changes role from {} to {} at term {} for {}",
        getMemberId(), this.role, newRole, state.getCurrentTerm(), reason);
    this.role.transitionRole(newRole);
  }

  boolean start() throws IOException {
    if (!lifeCycle.compareAndTransition(State.NEW, State.STARTING)) {
      return false;
    }
    final RaftConfiguration conf = getRaftConf();

    if (conf != null && conf.containsInBothConfs(getId())) {
      LOG.info("{}: start as a follower, conf={}", getMemberId(), conf);
      startAsPeer(RaftPeerRole.FOLLOWER);
    } else if (conf != null && conf.containsInConf(getId(), RaftPeerRole.LISTENER)) {
      LOG.info("{}: start as a listener, conf={}", getMemberId(), conf);
      startAsPeer(RaftPeerRole.LISTENER);
    } else {
      LOG.info("{}: start with initializing state, conf={}", getMemberId(), conf);
      setRole(RaftPeerRole.FOLLOWER, "start");
    }
    //尝试启动ServerState
    startSeverState();
    this.scheduled.scheduleWithFixedDelay(this::checkServerState, 2,2, TimeUnit.SECONDS);
    jmxAdapter.registerMBean();
    return true;
  }

  private void checkServerState(){
    if(!startComplete.get()) {
      if (!vnodeLease.isValid()) {
        startSeverState();
      }
    }else{
      //存储健康检查
      boolean checkHealth = this.state.getStorage().checkHealth();
      if(!checkHealth){
        LOG.warn("Storage health check failed, will stop SeverState");
        stopSeverState();
      }
    }
  }

  protected void checkStorageMount() throws IOException {
    if(storageMount!=null&&!storageMount.isEmpty()){
      boolean find = false;
      Path path = Paths.get("/proc/mounts");
      if (path.toFile().exists()) {
        for(String line : Files.readAllLines(path)) {
          if (line.contains(" "+storageMount+" ")) {
            find = true;
            break;
          }
        }
        if(!find){
          throw new IOException("StorageMount not find.");
        }
      }
    }

  }

  /**
   * 尝试启动ServerState
   */
  public void startSeverState() {
    if(!startComplete.get()){
      if(stateStart.compareAndSet(false,true)){
        try {
          LOG.info("try start SeverState");
          checkStorageMount();
          state.initialize(stateMachine);
          state.start();
          CodeInjectionForTesting.execute(START_COMPLETE, getId(), null, role);
          if (startComplete.compareAndSet(false, true)) {
            LOG.info("{}: Successfully started.", getMemberId());
          }
        } catch (Exception e) {
          LOG.warn("startSeverState failed", e);
        }finally {
          stateStart.set(false);
        }
      }
    }
  }

  /**
   * 停止ServerState
   */
  public void stopSeverState() {
    if (startComplete.compareAndSet(true, false)) {
      LOG.info("stop SeverState");
      this.state.close();
      vnodeLease.extend();
      LOG.info("{}: Successfully stopped.", getMemberId());
    }
  }

  /**
   * The peer belongs to the current configuration, should start as a follower or listener
   */
  private void startAsPeer(RaftPeerRole newRole) {
    final Object reason;
    if (newRole == RaftPeerRole.FOLLOWER) {
      reason = "startAsFollower";
      setRole(RaftPeerRole.FOLLOWER, reason);
    } else if (newRole == RaftPeerRole.LISTENER) {
      reason = "startAsListener";
      setRole(RaftPeerRole.LISTENER, reason);
    } else {
      throw new IllegalArgumentException("Unexpected role " + newRole);
    }
    role.startFollowerState(this, reason);

    lifeCycle.transition(State.RUNNING);
  }

  ServerState getState() {
    return state;
  }

  @Override
  public RaftGroupMemberId getMemberId() {
    return getState().getMemberId();
  }

  @Override
  public RaftPeer getPeer() {
    return Optional.ofNullable(getState().getCurrentPeer())
        .orElseGet(() -> getRaftServer().getPeer());
  }

  @Override
  public DivisionInfo getInfo() {
    return info;
  }

  RoleInfo getRole() {
    return role;
  }

  @Override
  public RaftConfiguration getRaftConf() {
    return getState().getRaftConf();
  }

  /**
   * This removes the group from the server.
   * If the deleteDirectory flag is set to false, and renameDirectory
   * the directory is moved to
   * {@link RaftServerConfigKeys#REMOVED_GROUPS_DIR_KEY} location.
   * If the deleteDirectory flag is true, the group is permanently deleted.
   */
  void groupRemove(boolean deleteDirectory, boolean renameDirectory) {
    final RaftStorageDirectory dir = state.getStorage().getStorageDir();

    /* Shutdown is triggered here inorder to avoid any locked files. */
    state.getStateMachineUpdater().setRemoving();
    close();
    try {
      closeFinishedLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("{}: Waiting closing interrupted, will not continue to remove group locally", getMemberId());
      return;
    }
    getStateMachine().event().notifyGroupRemove();
    if (deleteDirectory) {
      for (int i = 0; i < FileUtils.NUM_ATTEMPTS; i ++) {
        try {
          FileUtils.deleteFully(dir.getRoot());
          LOG.info("{}: Succeed to remove RaftStorageDirectory {}", getMemberId(), dir);
          break;
        } catch (NoSuchFileException e) {
          LOG.warn("{}: Some file does not exist {}", getMemberId(), dir, e);
        } catch (Exception e) {
          LOG.error("{}: Failed to remove RaftStorageDirectory {}", getMemberId(), dir, e);
          break;
        }
      }
    } else if(renameDirectory) {
      try {
        /* Create path with current group in REMOVED_GROUPS_DIR_KEY location */
        File toBeRemovedGroupFolder = new File(RaftServerConfigKeys
            .removedGroupsDir(proxy.getProperties()),
            dir.getRoot().getName());

        FileUtils.moveDirectory(dir.getRoot().toPath(),
            toBeRemovedGroupFolder.toPath());

        LOG.info("{}: Group {} is renamed successfully", getMemberId(), getGroup());
      } catch (IOException e) {
        LOG.warn("{}: Failed to remove group {}", getMemberId(),
            dir.getRoot().getName(), e);
      }
    }
  }

  @Override
  public void close() {
    lifeCycle.checkStateAndClose(() -> {
      LOG.info("{}: shutdown", getMemberId());
      try {
        jmxAdapter.unregister();
      } catch (Exception e) {
        LOG.warn("{}: Failed to un-register RaftServer JMX bean", getMemberId(), e);
      }
      try {
        role.shutdownFollowerState();
      } catch (Exception e) {
        LOG.warn("{}: Failed to shutdown FollowerState", getMemberId(), e);
      }
      try{
        role.shutdownLeaderElection();
      } catch (Exception e) {
        LOG.warn("{}: Failed to shutdown LeaderElection", getMemberId(), e);
      }
      try{
        role.shutdownLeaderState(true).join();
      } catch (Exception e) {
        LOG.warn("{}: Failed to shutdown LeaderState monitor", getMemberId(), e);
      }
      try{
        state.close();
      } catch (Exception e) {
        LOG.warn("{}: Failed to close state", getMemberId(), e);
      }
      try {
        leaderElectionMetrics.unregister();
        raftServerMetrics.unregister();
        RaftServerMetricsImpl.removeRaftServerMetrics(getMemberId());
      } catch (Exception e) {
        LOG.warn("{}: Failed to unregister metric", getMemberId(), e);
      }
      try {
        Concurrents3.shutdownAndWait(clientExecutor);
      } catch (Exception e) {
        LOG.warn(getMemberId() + ": Failed to shutdown clientExecutor", e);
      }
      try {
        Concurrents3.shutdownAndWait(serverExecutor);
      } catch (Exception e) {
        LOG.warn(getMemberId() + ": Failed to shutdown serverExecutor", e);
      }
      try {
        Concurrents3.shutdownAndWait(scheduled);
      } catch (Exception e) {
        LOG.warn(getMemberId() + ": Failed to shutdown scheduled", e);
      }
      closeFinishedLatch.countDown();
    });
  }

  void setFirstElection(Object reason) {
    if (firstElectionSinceStartup.compareAndSet(true, false)) {
      LOG.info("{}: set firstElectionSinceStartup to false for {}", getMemberId(), reason);
    }
  }

  /**
   * 如果当前服务器的角色不同或 force 参数为 true，则将服务器状态切换为 Follower。
   * @param newTerm 新的任期。
   * @param force 即使当前服务器已经是 Follower，也强制启动一个新的 {@link FollowerState}。
   *
   */
  private synchronized CompletableFuture<Void> changeToFollower(
      long newTerm, boolean force, boolean allowListener, Object reason, AtomicBoolean metadataUpdated) {
    final RaftPeerRole old = role.getCurrentRole();
    if (old == RaftPeerRole.LISTENER && !allowListener) {
      throw new IllegalStateException("Unexpected role " + old);
    }
    CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
    if ((old != RaftPeerRole.FOLLOWER || force) && old != RaftPeerRole.LISTENER) {
      setRole(RaftPeerRole.FOLLOWER, reason);
      if (old == RaftPeerRole.LEADER) {
        future = role.shutdownLeaderState(false)
            .exceptionally(e -> {
              if (e != null) {
                if (!getInfo().isAlive()) {
                  LOG.info("Since server is not alive {}, safely ignore {}", this, e.toString());
                  return null;
                }
              }
              throw new CompletionException("Failed to shutdownLeaderState: " + this, e);
            });
        state.setLeader(null, reason);
      } else if (old == RaftPeerRole.CANDIDATE) {
        future = role.shutdownLeaderElection();
      } else if (old == RaftPeerRole.FOLLOWER) {
        future = role.shutdownFollowerState();
      }

      metadataUpdated.set(state.updateCurrentTerm(newTerm));
      role.startFollowerState(this, reason);
      setFirstElection(reason);
    } else {
      metadataUpdated.set(state.updateCurrentTerm(newTerm));
    }
    return future;
  }

  synchronized CompletableFuture<Void> changeToFollowerAndPersistMetadata(
      long newTerm,
      boolean allowListener,
      Object reason) throws IOException {
    final AtomicBoolean metadataUpdated = new AtomicBoolean();
    final CompletableFuture<Void> future = changeToFollower(newTerm, false, allowListener, reason, metadataUpdated);
    if (metadataUpdated.get()) {
      state.persistMetadata();
    }
    return future;
  }

  synchronized void changeToLeader() {
    Preconditions.assertTrue(getInfo().isCandidate());
    role.shutdownLeaderElection();
    setRole(RaftPeerRole.LEADER, "changeToLeader");
    final LeaderStateImpl leader = role.updateLeaderState(this);
    state.becomeLeader();

    // start sending AppendEntries RPC to followers
    leader.start();
  }

  @Override
  public Collection<CommitInfoProto> getCommitInfos() {
    final List<CommitInfoProto> infos = new ArrayList<>();
    // add the commit info of this server
    final long commitIndex = updateCommitInfoCache();
    infos.add(ProtoUtils.toCommitInfoProto(getPeer(), commitIndex));

    // add the commit infos of other servers
    if (getInfo().isLeader()) {
      role.getLeaderState().ifPresent(
          leader -> leader.updateFollowerCommitInfos(commitInfoCache, infos));
    } else {
      RaftConfiguration raftConf = getRaftConf();
      Stream.concat(
              raftConf.getAllPeers(RaftPeerRole.FOLLOWER).stream(),
              raftConf.getAllPeers(RaftPeerRole.LISTENER).stream())
          .filter(peer -> !peer.getId().equals(getId()))
          .map(peer -> commitInfoCache.get(peer.getId())
              .map(index -> ProtoUtils.toCommitInfoProto(peer, index))
              .orElse(null))
          .filter(Objects::nonNull)
          .forEach(infos::add);
    }
    return infos;
  }

  GroupInfoReply getGroupInfo(GroupInfoRequest request) {
    final RaftStorageDirectory dir = state.getStorage().getStorageDir();
    final RaftConfigurationProto conf =
        LogProtoUtils.toRaftConfigurationProtoBuilder(getRaftConf()).build();
    return new GroupInfoReply(request, getCommitInfos(), getGroup(), getRoleInfoProto(),
        dir.isHealthy(), conf, getLogInfo(), getVnPeerId(), startComplete.get());
  }

  private String getVnPeerId() {
    return role.getLeaderState()
      .map(LeaderState::getVnPeerId)
      .orElse("");
  }

  LogInfoProto getLogInfo(){
    LogInfoProto.Builder logInfoBuilder = LogInfoProto.newBuilder();
    if(startComplete.get()) {
      final RaftLog log = getRaftLog();
      final TermIndex applied = getStateMachine().getLastAppliedTermIndex();
      if (applied != null) {
        logInfoBuilder.setApplied(applied.toProto());
      }
      final TermIndex committed = log.getTermIndex(log.getLastCommittedIndex());
      if (committed != null) {
        logInfoBuilder.setCommitted(committed.toProto());
      }
      final TermIndex entry = log.getLastEntryTermIndex();
      if (entry != null) {
        logInfoBuilder.setLastEntry(entry.toProto());
      }
      final SnapshotInfo snapshot = getStateMachine().getLatestSnapshot();
      if (snapshot != null) {
        logInfoBuilder.setLastSnapshot(snapshot.getTermIndex().toProto());
      }
    }
    return logInfoBuilder.build();
  }

  RoleInfoProto getRoleInfoProto() {
    return role.buildRoleInfoProto(this);
  }

  synchronized void changeToCandidate(boolean forceStartLeaderElection) {
    Preconditions.assertTrue(getInfo().isFollower());
    role.shutdownFollowerState();
    setRole(RaftPeerRole.CANDIDATE, "changeToCandidate");
    //引发变为候选人事件
    stateMachine.event().changeToCandidate(getMemberId());
    if (state.shouldNotifyExtendedNoLeader()) {
      stateMachine.followerEvent().notifyExtendedNoLeader(getRoleInfoProto());
    }
    // start election
    role.startLeaderElection(this, forceStartLeaderElection);
  }

  @Override
  public String toString() {
    return role + " " + state + " " + lifeCycle.getCurrentState();
  }

  RaftClientReply.Builder newReplyBuilder(RaftClientRequest request) {
    return RaftClientReply.newBuilder()
        .setRequest(request)
        .setCommitInfos(getCommitInfos());
  }

  private RaftClientReply.Builder newReplyBuilder(ClientInvocationId invocationId, long logIndex) {
    return RaftClientReply.newBuilder()
        .setClientInvocationId(invocationId)
        .setLogIndex(logIndex)
        .setServerId(getMemberId())
        .setCommitInfos(getCommitInfos());
  }

  RaftClientReply newSuccessReply(RaftClientRequest request) {
    return newReplyBuilder(request)
        .setSuccess()
        .build();
  }

  RaftClientReply newSuccessReply(RaftClientRequest request, long logIndex) {
    return newReplyBuilder(request)
        .setSuccess()
        .setLogIndex(logIndex)
        .build();
  }

  RaftClientReply newExceptionReply(RaftClientRequest request, RaftException exception) {
    return newReplyBuilder(request)
        .setException(exception)
        .build();
  }

  private CompletableFuture<RaftClientReply> checkLeaderState(RaftClientRequest request) {
    return checkLeaderState(request, null);
  }

  /**
   * @return null if the server is in leader state.
   */
  private CompletableFuture<RaftClientReply> checkLeaderState(RaftClientRequest request, CacheEntry entry) {
    try {
      assertGroup(getMemberId(), request);
    } catch (GroupMismatchException e) {
      return RetryCacheImpl.failWithException(e, entry);
    }

    if (!getInfo().isLeader()) {
      NotLeaderException exception = generateNotLeaderException();
      final RaftClientReply reply = newExceptionReply(request, exception);
      return RetryCacheImpl.failWithReply(reply, entry);
    }
    if (!getInfo().isLeaderReady()) {
      final CacheEntry cacheEntry = retryCache.getIfPresent(ClientInvocationId.valueOf(request));
      if (cacheEntry != null && cacheEntry.isCompletedNormally()) {
        return cacheEntry.getReplyFuture();
      }
      final LeaderNotReadyException lnre = new LeaderNotReadyException(getMemberId());
      final RaftClientReply reply = newExceptionReply(request, lnre);
      return RetryCacheImpl.failWithReply(reply, entry);
    }

    if (!request.isReadOnly() && isSteppingDown()) {
      final LeaderSteppingDownException lsde = new LeaderSteppingDownException(getMemberId() + " is stepping down");
      final RaftClientReply reply = newExceptionReply(request, lsde);
      return RetryCacheImpl.failWithReply(reply, entry);
    }

    return null;
  }

  NotLeaderException generateNotLeaderException() {
    if (!lifeCycle.getCurrentState().isRunning()) {
      return new NotLeaderException(getMemberId(), null, null);
    }
    RaftPeerId leaderId = state.getLeaderId();
    if (leaderId == null || leaderId.equals(getId())) {
      // No idea about who is the current leader. Or the peer is the current
      // leader, but it is about to step down. set the suggested leader as null.
      leaderId = null;
    }
    final RaftConfiguration conf = getRaftConf();
    Collection<RaftPeer> peers = conf.getAllPeers();
    return new NotLeaderException(getMemberId(), conf.getPeer(leaderId), peers);
  }

  void assertLifeCycleState(Set<LifeCycle.State> expected) throws ServerNotReadyException {
    lifeCycle.assertCurrentState((n, c) -> new ServerNotReadyException(
        getMemberId() + " is not in " + expected + ": current state is " + c), expected);
  }

  /**
   * 将一个事务追加到日志中，以处理客户端请求。
   * 注意，给定的请求可能与 {@link TransactionContext#getClientRequest()} 不同，
   * 因为请求可能会被转换；参见 {@link #convertRaftClientRequest(RaftClientRequest)}。
   * 其核心逻辑如下：
   *   1. 参数校验与调试注入：检查请求非空，并允许测试时注入代码。
   *   2. 状态检查：通过 checkLeaderState 确保当前节点是合法的 Leader，否则返回错误响应。
   *   3. 获取 Leader 状态：确保当前节点处于 Leader 角色，并准备写入日志。
   *   4. 资源许可控制：尝试获取写入许可（PendingRequests.Permit），失败则返回资源不可用异常。
   *   5. 日志追加：调用 state.appendLog(context) 将事务上下文写入日志。
   *   6. 异常处理：
   *      若发生 StateMachineException，构造异常响应并提交 Leader 下台事件（如需）。
   *      若发生 ServerNotReadyException，直接返回服务未就绪异常。
   *   7. 添加待处理请求：将请求和上下文封装为 PendingRequest 并加入队列。
   *   8. 通知发送者：触发日志复制流程。
   *   9. 返回结果：返回 pending.getFuture()，等待后续日志提交完成后的响应。
   * @param request 客户端请求。
   * @param context 事务的上下文。
   * @param cacheEntry 重试缓存中的条目。
   * @return 回复的 Future 对象。
   */
  private CompletableFuture<RaftClientReply> appendTransaction(
      RaftClientRequest request, TransactionContextImpl context, CacheEntry cacheEntry) {
    Objects.requireNonNull(request, "request == null");
    CodeInjectionForTesting.execute(APPEND_TRANSACTION, getId(),
        request.getClientId(), request, context, cacheEntry);

    final PendingRequest pending;
    synchronized (this) {
      final CompletableFuture<RaftClientReply> reply = checkLeaderState(request, cacheEntry);
      if (reply != null) {
        return reply;
      }

      // append the message to its local log
      final LeaderStateImpl leaderState = role.getLeaderStateNonNull();
      writeIndexCache.add(request.getClientId(), context.getLogIndexFuture());

      final PendingRequests.Permit permit = leaderState.tryAcquirePendingRequest(request.getMessage());
      if (permit == null) {
        cacheEntry.failWithException(new ResourceUnavailableException(
            getMemberId() + ": Failed to acquire a pending write request for " + request));
        return cacheEntry.getReplyFuture();
      }
      try {
        assertLifeCycleState(LifeCycle.States.RUNNING);
        state.appendLog(context);
      } catch (StateMachineException e) {
        // the StateMachineException is thrown by the SM in the preAppend stage.
        // Return the exception in a RaftClientReply.
        RaftClientReply exceptionReply = newExceptionReply(request, e);
        cacheEntry.failWithReply(exceptionReply);
        // leader will step down here
        if (e.leaderShouldStepDown() && getInfo().isLeader()) {
          leaderState.submitStepDownEvent(LeaderState.StepDownReason.STATE_MACHINE_EXCEPTION);
        }
        return CompletableFuture.completedFuture(exceptionReply);
      } catch (ServerNotReadyException e) {
        final RaftClientReply exceptionReply = newExceptionReply(request, e);
        return CompletableFuture.completedFuture(exceptionReply);
      }

      // 将请求放入待处理队列中
      pending = leaderState.addPendingRequest(permit, request, context);
      if (pending == null) {
        cacheEntry.failWithException(new ResourceUnavailableException(
            getMemberId() + ": Failed to add a pending write request for " + request));
        return cacheEntry.getReplyFuture();
      }
      leaderState.notifySenders();
    }

    return pending.getFuture();
  }

  /** Wait until the given replication requirement is satisfied. */
  private CompletableFuture<RaftClientReply> waitForReplication(RaftClientReply reply, ReplicationLevel replication) {
    if (!reply.isSuccess()) {
      return CompletableFuture.completedFuture(reply);
    }
    final RaftClientRequest.Type type = RaftClientRequest.watchRequestType(reply.getLogIndex(), replication);
    final RaftClientRequest watch = RaftClientRequest.newBuilder()
        .setServerId(reply.getServerId())
        .setClientId(reply.getClientId())
        .setGroupId(reply.getRaftGroupId())
        .setCallId(reply.getCallId())
        .setType(type)
        .build();
    return watchAsync(watch).thenApply(watchReply -> combineReplies(reply, watchReply));
  }

  private RaftClientReply combineReplies(RaftClientReply reply, RaftClientReply watchReply) {
    final RaftClientReply combinedReply = RaftClientReply.newBuilder()
        .setServerId(getMemberId())
        // from write reply
        .setClientId(reply.getClientId())
        .setCallId(reply.getCallId())
        .setMessage(reply.getMessage())
        .setLogIndex(reply.getLogIndex())
        // from watchReply
        .setSuccess(watchReply.isSuccess())
        .setException(watchReply.getException())
        .setCommitInfos(watchReply.getCommitInfos())
        .build();
    LOG.debug("combinedReply={}", combinedReply);
    return combinedReply;
  }

  void stepDownOnJvmPause() {
    role.getLeaderState().ifPresent(leader -> leader.submitStepDownEvent(LeaderState.StepDownReason.JVM_PAUSE));
  }

  /** If the given request is {@link TypeCase#FORWARD}, convert it. */
  static RaftClientRequest convertRaftClientRequest(RaftClientRequest request) throws InvalidProtocolBufferException {
    if (!request.is(TypeCase.FORWARD)) {
      return request;
    }
    return ClientProtoUtils.toRaftClientRequest(RaftClientRequestProto.parseFrom(
        request.getMessage().getContent().asReadOnlyByteBuffer()));
  }

  <REPLY> CompletableFuture<REPLY> executeSubmitServerRequestAsync(
      CheckedSupplier<CompletableFuture<REPLY>, IOException> submitFunction) {
    return CompletableFuture.supplyAsync(
        () -> JavaUtils.callAsUnchecked(submitFunction, CompletionException::new),
        serverExecutor).join();
  }

  CompletableFuture<RaftClientReply> executeSubmitClientRequestAsync(
      ReferenceCountedObject<RaftClientRequest> request) {
    return CompletableFuture.supplyAsync(() -> submitClientRequestAsync(request), clientExecutor).join();
  }

  /**
   * 该函数 submitClientRequestAsync 的作用是异步处理客户端提交的 Raft 请求，
   * 其主要功能如下：
   *   1. 保留请求引用并记录日志：通过 requestRef.retain() 保留请求对象，防止在处理过程中被释放，并记录调试日志。
   *   2. 检查服务器生命周期状态：调用 assertLifeCycleState(LifeCycle.States.RUNNING) 确保服务器处于运行状态，否则构造异常响应返回给客户端。
   *   3. 启动性能计时器：根据请求类型获取对应的指标计时器（如读、写等），用于统计请求耗时。
   *   4. 执行请求处理链：调用 replyFuture(requestRef) 启动实际的请求处理流程，返回一个包含响应的 CompletableFuture。
   *   5. 完成时停止计时与错误统计：无论请求成功或失败，都会停止计时器，并在发生异常时增加失败计数器。
   *   6. 最终释放请求资源：在 finally 块中调用 requestRef.release() 释放请求引用。
   */
  @Override
  public CompletableFuture<RaftClientReply> submitClientRequestAsync(
      ReferenceCountedObject<RaftClientRequest> requestRef) {
    final RaftClientRequest request = requestRef.retain();
    LOG.debug("{}: receive client request({})", getMemberId(), request);

    try {
      assertLifeCycleState(LifeCycle.States.RUNNING);
    } catch (ServerNotReadyException e) {
      final RaftClientReply reply = newExceptionReply(request, e);
      requestRef.release();
      return CompletableFuture.completedFuture(reply);
    }

    try {
      RaftClientRequest.Type type = request.getType();
      final Timekeeper timer = raftServerMetrics.getClientRequestTimer(type);
      final Optional<Timekeeper.Context> timerContext = Optional.ofNullable(timer).map(Timekeeper::time);
      return replyFuture(requestRef).whenComplete((clientReply, exception) -> {
        timerContext.ifPresent(Timekeeper.Context::stop);
        if (exception != null || clientReply.getException() != null) {
          raftServerMetrics.incFailedRequestCount(type);
        }
      });
    } finally {
      requestRef.release();
    }
  }

  private CompletableFuture<RaftClientReply> replyFuture(ReferenceCountedObject<RaftClientRequest> requestRef) {
    final RaftClientRequest request = requestRef.get();
    retryCache.invalidateRepliedRequests(request);

    final TypeCase type = request.getType().getTypeCase();
    switch (type) {
      case STALEREAD:
        return staleReadAsync(request);
      case READ:
        return readAsync(request);
      case WATCH:
        return watchAsync(request);
      case MESSAGESTREAM:
        return messageStreamAsync(requestRef);
      case WRITE:
      case FORWARD:
        return writeAsync(requestRef);
      default:
        throw new IllegalStateException("Unexpected request type: " + type + ", request=" + request);
    }
  }

  /**
   * 该函数 writeAsync 的作用是异步处理写请求，
   * 并根据复制级别决定是否等待特定的复制完成：
   *   1. 获取请求对象：从引用中获取实际的 RaftClientRequest 请求。
   *   2. 执行写操作：调用 writeAsyncImpl 执行实际的异步写逻辑。
   *   3. 检查复制级别：
   *      如果请求是写操作且复制级别不是 MAJORITY，则继续调用 waitForReplication 等待指定复制级别达成。
   *   4. 返回结果：最终返回带有写结果或复制完成后的响应的 CompletableFuture
   */
  private CompletableFuture<RaftClientReply> writeAsync(ReferenceCountedObject<RaftClientRequest> requestRef) {
    final RaftClientRequest request = requestRef.get();
    final CompletableFuture<RaftClientReply> future = writeAsyncImpl(requestRef);
    if (request.is(TypeCase.WRITE)) {
      // check replication
      final ReplicationLevel replication = request.getType().getWrite().getReplication();
      if (replication != ReplicationLevel.MAJORITY) {
        return future.thenCompose(r -> waitForReplication(r, replication));
      }
    }
    return future;
  }

  /**
   * 该函数的主要流程如下：
   *   1. 检查领导状态：若节点不是Leader或处于不可用状态，返回对应的异常响应。
   *   2. 查询重试缓存：如果请求来自重试，直接返回缓存中的结果。
   *   3. 启动事务：调用状态机开始处理事务，若失败则记录异常并返回错误响应。
   *   4. 提交事务：将请求、上下文和缓存条目一起提交到日志中进行后续处理。
   */
  private CompletableFuture<RaftClientReply> writeAsyncImpl(ReferenceCountedObject<RaftClientRequest> requestRef) {
    final RaftClientRequest request = requestRef.get();
    final CompletableFuture<RaftClientReply> reply = checkLeaderState(request);
    if (reply != null) {
      return reply;
    }

    // query the retry cache
    final RetryCacheImpl.CacheQueryResult queryResult = retryCache.queryCache(request);
    final CacheEntry cacheEntry = queryResult.getEntry();
    if (queryResult.isRetry()) {
      // return the cached future.
      return cacheEntry.getReplyFuture();
    }
    // TODO: this client request will not be added to pending requests until
    // later which means that any failure in between will leave partial state in
    // the state machine. We should call cancelTransaction() for failed requests
    final TransactionContextImpl context;
    try {
      context = (TransactionContextImpl) stateMachine.startTransaction(convertRaftClientRequest(request));
    } catch (IOException e) {
      final RaftClientReply exceptionReply = newExceptionReply(request,
          new RaftException("Failed to startTransaction for " + request, e));
      cacheEntry.failWithReply(exceptionReply);
      return CompletableFuture.completedFuture(exceptionReply);
    }
    if (context.getException() != null) {
      final StateMachineException e = new StateMachineException(getMemberId(), context.getException());
      final RaftClientReply exceptionReply = newExceptionReply(request, e);
      cacheEntry.failWithReply(exceptionReply);
      return CompletableFuture.completedFuture(exceptionReply);
    }

    context.setDelegatedRef(requestRef);
    return appendTransaction(request, context, cacheEntry);
  }

  private CompletableFuture<RaftClientReply> watchAsync(RaftClientRequest request) {
    final CompletableFuture<RaftClientReply> reply = checkLeaderState(request);
    if (reply != null) {
      return reply;
    }

    return role.getLeaderState()
        .map(ls -> ls.addWatchRequest(request))
        .orElseGet(() -> CompletableFuture.completedFuture(
            newExceptionReply(request, generateNotLeaderException())));
  }

  private CompletableFuture<RaftClientReply> staleReadAsync(RaftClientRequest request) {
    final long minIndex = request.getType().getStaleRead().getMinIndex();
    final long commitIndex = state.getLog().getLastCommittedIndex();
    LOG.debug("{}: minIndex={}, commitIndex={}", getMemberId(), minIndex, commitIndex);
    if (commitIndex < minIndex) {
      final StaleReadException e = new StaleReadException(
          "Unable to serve stale-read due to server commit index = " + commitIndex + " < min = " + minIndex);
      return CompletableFuture.completedFuture(
          newExceptionReply(request, new StateMachineException(getMemberId(), e)));
    }
    return processQueryFuture(stateMachine.queryStale(request.getMessage(), minIndex), request);
  }

  ReadRequests getReadRequests() {
    return getState().getReadRequests();
  }

  private CompletableFuture<ReadIndexReplyProto> sendReadIndexAsync(RaftClientRequest clientRequest) {
    final RaftPeerId leaderId = getInfo().getLeaderId();
    if (leaderId == null) {
      return JavaUtils.completeExceptionally(new ReadIndexException(getMemberId() + ": Leader is unknown."));
    }
    final ReadIndexRequestProto request = toReadIndexRequestProto(clientRequest, getMemberId(), leaderId);
    try {
      return getServerRpc().async().readIndexAsync(request);
    } catch (IOException e) {
      return JavaUtils.completeExceptionally(e);
    }
  }

  private CompletableFuture<Long> getReadIndex(RaftClientRequest request, LeaderStateImpl leader) {
    return writeIndexCache.getWriteIndexFuture(request).thenCompose(leader::getReadIndex);
  }

  private CompletableFuture<RaftClientReply> readAsync(RaftClientRequest request) {
    if (request.getType().getRead().getPreferNonLinearizable()
        || readOption == RaftServerConfigKeys.Read.Option.DEFAULT) {
      final CompletableFuture<RaftClientReply> reply = checkLeaderState(request);
       if (reply != null) {
         return reply;
       }
       return queryStateMachine(request);
    } else if (readOption == RaftServerConfigKeys.Read.Option.LINEARIZABLE){
      /*
        Linearizable read using ReadIndex. See Raft paper section 6.4.
        1. First obtain readIndex from Leader.
        2. Then waits for statemachine to advance at least as far as readIndex.
        3. Finally, query the statemachine and return the result.
       */
      final LeaderStateImpl leader = role.getLeaderState().orElse(null);

      final CompletableFuture<Long> replyFuture;
      if (leader != null) {
        replyFuture = getReadIndex(request, leader);
      } else {
        replyFuture = sendReadIndexAsync(request).thenApply(reply   -> {
          if (reply.getServerReply().getSuccess()) {
            return reply.getReadIndex();
          } else {
            throw new CompletionException(new ReadIndexException(getId() +
                ": Failed to get read index from the leader: " + reply));
          }
        });
      }

      return replyFuture
          .thenCompose(readIndex -> getReadRequests().waitToAdvance(readIndex))
          .thenCompose(readIndex -> queryStateMachine(request))
          .exceptionally(e -> readException2Reply(request, e));
    } else {
      throw new IllegalStateException("Unexpected read option: " + readOption);
    }
  }

  private RaftClientReply readException2Reply(RaftClientRequest request, Throwable e) {
    e = JavaUtils.unwrapCompletionException(e);
    if (e instanceof StateMachineException ) {
      return newExceptionReply(request, (StateMachineException) e);
    } else if (e instanceof ReadException) {
      return newExceptionReply(request, (ReadException) e);
    } else if (e instanceof ReadIndexException) {
      return newExceptionReply(request, (ReadIndexException) e);
    } else {
      throw new CompletionException(e);
    }
  }

  private CompletableFuture<RaftClientReply> messageStreamAsync(ReferenceCountedObject<RaftClientRequest> requestRef) {
    final RaftClientRequest request = requestRef.get();
    final CompletableFuture<RaftClientReply> reply = checkLeaderState(request);
    if (reply != null) {
      return reply;
    }

    if (request.getType().getMessageStream().getEndOfRequest()) {
      final CompletableFuture<ReferenceCountedObject<RaftClientRequest>> f = streamEndOfRequestAsync(requestRef);
      if (f.isCompletedExceptionally()) {
        return f.thenApply(r -> null);
      }
      // the message stream has ended and the request become a WRITE request
      ReferenceCountedObject<RaftClientRequest> joinedRequest = f.join();
      try {
        return replyFuture(joinedRequest);
      } finally {
        // Released pending streaming requests.
        joinedRequest.release();
      }
    }

    return role.getLeaderState()
        .map(ls -> ls.streamAsync(requestRef))
        .orElseGet(() -> CompletableFuture.completedFuture(
            newExceptionReply(request, generateNotLeaderException())));
  }

  private CompletableFuture<ReferenceCountedObject<RaftClientRequest>> streamEndOfRequestAsync(
      ReferenceCountedObject<RaftClientRequest> request) {
    return role.getLeaderState()
        .map(ls -> ls.streamEndOfRequestAsync(request))
        .orElse(null);
  }

  CompletableFuture<RaftClientReply> queryStateMachine(RaftClientRequest request) {
    return processQueryFuture(stateMachine.query(request.getMessage()), request);
  }

  CompletableFuture<RaftClientReply> processQueryFuture(
      CompletableFuture<Message> queryFuture, RaftClientRequest request) {
    return queryFuture.thenApply(r -> newReplyBuilder(request).setSuccess().setMessage(r).build())
        .exceptionally(e -> {
          e = JavaUtils.unwrapCompletionException(e);
          if (e instanceof StateMachineException) {
            return newExceptionReply(request, (StateMachineException)e);
          }
          throw new CompletionException(e);
        });
  }

  @Override
  public RaftClientReply submitClientRequest(RaftClientRequest request)
      throws IOException {
    return waitForReply(request, submitClientRequestAsync(request));
  }

  RaftClientReply waitForReply(RaftClientRequest request, CompletableFuture<RaftClientReply> future)
      throws IOException {
    return waitForReply(getMemberId(), request, future, e -> newExceptionReply(request, e));
  }

  static <REPLY extends RaftClientReply> REPLY waitForReply(
      Object id, RaftClientRequest request, CompletableFuture<REPLY> future,
      Function<RaftException, REPLY> exceptionReply)
      throws IOException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      final String s = id + ": Interrupted when waiting for reply, request=" + request;
      LOG.info(s, e);
      Thread.currentThread().interrupt();
      throw IOUtils.toInterruptedIOException(s, e);
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause == null) {
        throw new IOException(e);
      }
      if (cause instanceof NotLeaderException ||
          cause instanceof StateMachineException) {
        final REPLY reply = exceptionReply.apply((RaftException) cause);
        if (reply != null) {
          return reply;
        }
      }
      throw IOUtils.asIOException(cause);
    }
  }

  RaftClientReply transferLeadership(TransferLeadershipRequest request) throws IOException {
    return waitForReply(request, transferLeadershipAsync(request));
  }

  private CompletableFuture<RaftClientReply> logAndReturnTransferLeadershipFail(
      TransferLeadershipRequest request, String msg) {
    LOG.warn(msg);
    return CompletableFuture.completedFuture(
        newExceptionReply(request, new TransferLeadershipException(msg)));
  }

  boolean isSteppingDown() {
    return transferLeadership.isSteppingDown();
  }

  /**
   * 该函数的作用是异步地将 Raft 集群中的领导权转移给指定的节点（request.getNewLeader()）。
   * 其主要逻辑如下：
   *   1. 如果新领导者为空，则执行下台当前领导的操作。
   *   2. 检查服务器生命周期状态是否为运行中，并验证请求的组一致性。
   *   3. 检查当前节点是否为 Leader，若不是则返回错误。
   *   4. 确保没有正在进行的配置变更（reconfiguration）。
   *   5. 确保目标节点存在于当前配置中。
   *   6. 确保目标节点具有最高优先级。
   *   7. 若所有条件满足，则调用 transferLeadership.start() 开始领导权转移。
   */
  CompletableFuture<RaftClientReply> transferLeadershipAsync(TransferLeadershipRequest request)
      throws IOException {
    if (request.getNewLeader() == null) {
      return stepDownLeaderAsync(request);
    }

    LOG.info("{}: receive transferLeadership {}", getMemberId(), request);
    assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(getMemberId(), request);

    synchronized (this) {
      CompletableFuture<RaftClientReply> reply = checkLeaderState(request);
      if (reply != null) {
        return reply;
      }

      if (getId().equals(request.getNewLeader())) {
        return CompletableFuture.completedFuture(newSuccessReply(request));
      }

      final RaftConfiguration conf = getRaftConf();
      final LeaderStateImpl leaderState = role.getLeaderStateNonNull();

      // make sure there is no raft reconfiguration in progress
      if (!conf.isStable() || leaderState.inStagingState() || !state.isConfCommitted()) {
        String msg = getMemberId() + " refused to transfer leadership to peer " + request.getNewLeader() +
            " when raft reconfiguration in progress.";
        return logAndReturnTransferLeadershipFail(request, msg);
      }

      if (!conf.containsInConf(request.getNewLeader())) {
        String msg = getMemberId() + " refused to transfer leadership to peer " + request.getNewLeader() +
            " as it is not in " + conf;
        return logAndReturnTransferLeadershipFail(request, msg);
      }

      if (!conf.isHighestPriority(request.getNewLeader())) {
        String msg = getMemberId() + " refused to transfer leadership to peer " + request.getNewLeader() +
            " as it does not has highest priority in " + conf;
        return logAndReturnTransferLeadershipFail(request, msg);
      }

      return transferLeadership.start(leaderState, request);
    }
  }

  CompletableFuture<RaftClientReply> takeSnapshotAsync(SnapshotManagementRequest request) throws IOException {
    LOG.info("{}: takeSnapshotAsync {}", getMemberId(), request);
    assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(getMemberId(), request);
    Preconditions.assertNotNull(request.getCreate(), "create");

    final long creationGap = request.getCreate().getCreationGap();
    long minGapValue = creationGap > 0? creationGap : RaftServerConfigKeys.Snapshot.creationGap(proxy.getProperties());
    final long lastSnapshotIndex = Optional.ofNullable(stateMachine.getLatestSnapshot())
        .map(SnapshotInfo::getIndex)
        .orElse(0L);
    if (state.getLastAppliedIndex() - lastSnapshotIndex < minGapValue) {
      return CompletableFuture.completedFuture(newSuccessReply(request, lastSnapshotIndex));
    }

    synchronized (this) {
      final long installSnapshot = snapshotInstallationHandler.getInProgressInstallSnapshotIndex();
      // check snapshot install/load
      if (installSnapshot != RaftLog.INVALID_LOG_INDEX) {
        String msg = String.format("%s: Failed do snapshot as snapshot (%s) installation is in progress",
            getMemberId(), installSnapshot);
        LOG.warn(msg);
        return CompletableFuture.completedFuture(newExceptionReply(request,new RaftException(msg)));
      }
      return snapshotRequestHandler.takingSnapshotAsync(request);
    }
  }

  SnapshotManagementRequestHandler getSnapshotRequestHandler() {
    return snapshotRequestHandler;
  }

  CompletableFuture<RaftClientReply> leaderElectionManagementAsync(LeaderElectionManagementRequest request)
      throws IOException {
    LOG.info("{} receive leaderElectionManagement request {}", getMemberId(), request);
    assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(getMemberId(), request);

    final LeaderElectionManagementRequest.Pause pause = request.getPause();
    if (pause != null) {
      getRole().setLeaderElectionPause(true);
      return CompletableFuture.completedFuture(newSuccessReply(request));
    }
    final LeaderElectionManagementRequest.Resume resume = request.getResume();
    if (resume != null) {
      getRole().setLeaderElectionPause(false);
      return CompletableFuture.completedFuture(newSuccessReply(request));
    }
    return JavaUtils.completeExceptionally(new UnsupportedOperationException(
        getId() + ": Request not supported " + request));
  }

  CompletableFuture<RaftClientReply> stepDownLeaderAsync(TransferLeadershipRequest request) throws IOException {
    LOG.info("{} receive stepDown leader request {}", getMemberId(), request);
    assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(getMemberId(), request);

    return role.getLeaderState().map(leader -> leader.submitStepDownRequestAsync(request))
        .orElseGet(() -> CompletableFuture.completedFuture(
            newExceptionReply(request, generateNotLeaderException())));
  }

  public RaftClientReply setConfiguration(SetConfigurationRequest request) throws IOException {
    return waitForReply(request, setConfigurationAsync(request));
  }

  /**
   * Handle a raft configuration change request from client.
   */
  public CompletableFuture<RaftClientReply> setConfigurationAsync(SetConfigurationRequest request) throws IOException {
    LOG.info("{}: receive setConfiguration {}", getMemberId(), request);
    assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(getMemberId(), request);

    CompletableFuture<RaftClientReply> reply = checkLeaderState(request);
    if (reply != null) {
      return reply;
    }

    final SetConfigurationRequest.Arguments arguments = request.getArguments();
    final PendingRequest pending;
    synchronized (this) {
      reply = checkLeaderState(request);
      if (reply != null) {
        return reply;
      }

      final RaftConfiguration current = getRaftConf();
      final LeaderStateImpl leaderState = role.getLeaderStateNonNull();
      // make sure there is no other raft reconfiguration in progress
      if (!current.isStable() || leaderState.inStagingState() || !state.isConfCommitted()) {
        throw new ReconfigurationInProgressException(
            "Reconfiguration is already in progress: " + current);
      }

      final List<RaftPeer> serversInNewConf;
      final List<RaftPeer> listenersInNewConf;
      if (arguments.getMode() == SetConfigurationRequest.Mode.ADD) {
        serversInNewConf = add(RaftPeerRole.FOLLOWER, current, arguments);
        listenersInNewConf = add(RaftPeerRole.LISTENER, current, arguments);
      } else if (arguments.getMode() == SetConfigurationRequest.Mode.COMPARE_AND_SET) {
        final Comparator<RaftPeer> comparator = Comparator.comparing(RaftPeer::getId,
            Comparator.comparing(RaftPeerId::toString));
        if (Collections3.equalsIgnoreOrder(arguments.getServersInCurrentConf(),
            current.getAllPeers(RaftPeerRole.FOLLOWER), comparator)
            && Collections3.equalsIgnoreOrder(arguments.getListenersInCurrentConf(),
            current.getAllPeers(RaftPeerRole.LISTENER), comparator)) {
          serversInNewConf = arguments.getPeersInNewConf(RaftPeerRole.FOLLOWER);
          listenersInNewConf = arguments.getPeersInNewConf(RaftPeerRole.LISTENER);
        } else {
          throw new SetConfigurationException("Failed to set configuration: current configuration "
              + current + " is different than the request " + request);
        }
      } else {
        serversInNewConf = arguments.getPeersInNewConf(RaftPeerRole.FOLLOWER);
        listenersInNewConf = arguments.getPeersInNewConf(RaftPeerRole.LISTENER);
      }

      // return success with a null message if the new conf is the same as the current
      if (current.hasNoChange(serversInNewConf, listenersInNewConf)) {
        pending = new PendingRequest(request);
        pending.setReply(newSuccessReply(request));
        return pending.getFuture();
      }
      if (current.changeMajority(serversInNewConf)) {
        if (!memberMajorityAddEnabled) {
          throw new SetConfigurationException("Failed to set configuration: request " + request
              + " changes a majority set of the current configuration " + current);
        }
        LOG.warn("Try to add/replace a majority of servers in a single setConf: {}", request);
      }

      getRaftServer().addRaftPeers(serversInNewConf);
      getRaftServer().addRaftPeers(listenersInNewConf);
      // add staging state into the leaderState
      pending = leaderState.startSetConfiguration(request, serversInNewConf);
    }
    return pending.getFuture();
  }

  static List<RaftPeer> add(RaftPeerRole role, RaftConfiguration conf, SetConfigurationRequest.Arguments args) {
    final Map<RaftPeerId, RaftPeer> inConfs = conf.getAllPeers(role).stream()
        .collect(Collectors.toMap(RaftPeer::getId, Function.identity()));

    final List<RaftPeer> toAdds = args.getPeersInNewConf(role);
    toAdds.stream().map(RaftPeer::getId).forEach(inConfs::remove);

    return Stream.concat(toAdds.stream(), inConfs.values().stream()).collect(Collectors.toList());
  }

  /**
   * The remote peer should shut down if all the following are true.
   * 1. this is the current leader
   * 2. current conf is stable and has been committed
   * 3. candidate is not in the current conf
   * 4. candidate last entry index < conf index (the candidate was removed)
   */
  private boolean shouldSendShutdown(RaftPeerId candidateId, TermIndex candidateLastEntry) {
    return getInfo().isLeader()
        && getRaftConf().isStable()
        && getState().isConfCommitted()
        && !getRaftConf().containsInConf(candidateId)
        && candidateLastEntry.getIndex() < getRaftConf().getLogEntryIndex()
        && role.getLeaderState().map(ls -> !ls.isBootStrappingPeer(candidateId)).orElse(false);
  }

  @Override
  public RequestVoteReplyProto requestVote(RequestVoteRequestProto r) throws IOException {
    final RaftRpcRequestProto request = r.getServerRequest();
    return requestVote(r.getPreVote() ? Phase.PRE_VOTE : Phase.ELECTION,
        RaftPeerId.valueOf(request.getRequestorId()),
        ProtoUtils.toRaftGroupId(request.getRaftGroupId()),
        r.getCandidateTerm(),
        TermIndex.valueOf(r.getCandidateLastEntry()));
  }

  private RequestVoteReplyProto requestVote(Phase phase,
      RaftPeerId candidateId, RaftGroupId candidateGroupId,
      long candidateTerm, TermIndex candidateLastEntry) throws IOException {
    CodeInjectionForTesting.execute(REQUEST_VOTE, getId(),
        candidateId, candidateTerm, candidateLastEntry);
    LOG.info("{}: receive requestVote({}, {}, {}, {}, {})",
        getMemberId(), phase, candidateId, candidateGroupId, candidateTerm, candidateLastEntry);
    assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(getMemberId(), candidateId, candidateGroupId);

    boolean shouldShutdown = false;
    final RequestVoteReplyProto reply;
    CompletableFuture<Void> future = null;
    synchronized (this) {
      // Check life cycle state again to avoid the PAUSING/PAUSED state.
      assertLifeCycleState(LifeCycle.States.RUNNING);

      final VoteContext context = new VoteContext(this, phase, candidateId);
      final RaftPeer candidate = context.recognizeCandidate(candidateTerm);
      final boolean voteGranted = context.decideVote(candidate, candidateLastEntry);
      if (candidate != null && phase == Phase.ELECTION) {
        // change server state in the ELECTION phase
        final AtomicBoolean termUpdated = new AtomicBoolean();
        future = changeToFollower(candidateTerm, true, false, "candidate:" + candidateId, termUpdated);
        if (voteGranted) {
          state.grantVote(candidate.getId());
        }
        if (termUpdated.get() || voteGranted) {
          state.persistMetadata(); // sync metafile
        }
      }
      if (voteGranted) {
        role.getFollowerState().ifPresent(fs -> fs.updateLastRpcTime(FollowerState.UpdateType.REQUEST_VOTE));
      } else if(shouldSendShutdown(candidateId, candidateLastEntry)) {
        shouldShutdown = true;
      }
      reply = toRequestVoteReplyProto(candidateId, getMemberId(),
          voteGranted, state.getCurrentTerm(), shouldShutdown);
      if (LOG.isInfoEnabled()) {
        LOG.info("{} replies to {} vote request: {}. Peer's state: {}",
            getMemberId(), phase, toRequestVoteReplyString(reply), state);
      }
    }
    if (future != null) {
      future.join();
    }
    return reply;
  }

  @Override
  public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto r)
      throws IOException {
    try {
      return appendEntriesAsync(ReferenceCountedObject.wrap(r)).join();
    } catch (CompletionException e) {
      throw IOUtils.asIOException(JavaUtils.unwrapCompletionException(e));
    }
  }

  @Override
  public CompletableFuture<AppendEntriesReplyProto> appendEntriesAsync(
      ReferenceCountedObject<AppendEntriesRequestProto> requestRef) throws IOException {
    final AppendEntriesRequestProto r = requestRef.retain();
    final RaftRpcRequestProto request = r.getServerRequest();
    final TermIndex previous = r.hasPreviousLog()? TermIndex.valueOf(r.getPreviousLog()) : null;
    try {
      final RaftPeerId leaderId = RaftPeerId.valueOf(request.getRequestorId());
      final RaftGroupId leaderGroupId = ProtoUtils.toRaftGroupId(request.getRaftGroupId());

      CodeInjectionForTesting.execute(APPEND_ENTRIES, getId(), leaderId, previous, r);

      assertLifeCycleState(LifeCycle.States.STARTING_OR_RUNNING);
//      if (!startComplete.get()) {
//        throw new ServerNotReadyException(getMemberId() + ": The server role is not yet initialized.");
//      }
      assertGroup(getMemberId(), leaderId, leaderGroupId);
      assertEntries(r, previous, state);

      return appendEntriesAsync(leaderId, request.getCallId(), previous, requestRef);
    } catch(Exception t) {
      LOG.error("{}: Failed appendEntries* {}", getMemberId(),
          toAppendEntriesRequestString(r, stateMachine::toStateMachineLogEntryString), t);
      throw IOUtils.asIOException(t);
    } finally {
      requestRef.release();
    }
  }

  @Override
  public CompletableFuture<ReadIndexReplyProto> readIndexAsync(ReadIndexRequestProto request) throws IOException {
    assertLifeCycleState(LifeCycle.States.RUNNING);

    final RaftPeerId peerId = RaftPeerId.valueOf(request.getServerRequest().getRequestorId());

    final LeaderStateImpl leader = role.getLeaderState().orElse(null);
    if (leader == null) {
      return CompletableFuture.completedFuture(toReadIndexReplyProto(peerId, getMemberId()));
    }

    return getReadIndex(ClientProtoUtils.toRaftClientRequest(request.getClientRequest()), leader)
        .thenApply(index -> toReadIndexReplyProto(peerId, getMemberId(), true, index))
        .exceptionally(throwable -> toReadIndexReplyProto(peerId, getMemberId()));
  }

  static void logAppendEntries(boolean isHeartbeat, Supplier<String> message) {
    if (isHeartbeat) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("HEARTBEAT: " + message.get());
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug(message.get());
      }
    }
  }

  Optional<FollowerState> updateLastRpcTime(FollowerState.UpdateType updateType) {
    final Optional<FollowerState> fs = role.getFollowerState();
    if (fs.isPresent() && lifeCycle.getCurrentState().isRunning()) {
      fs.get().updateLastRpcTime(updateType);
      return fs;
    } else {
      return Optional.empty();
    }
  }

  private long updateCommitInfoCache() {
    return commitInfoCache.update(getId(), state.getLog().getLastCommittedIndex());
  }

  ExecutorService getServerExecutor() {
    return serverExecutor;
  }

  private CompletableFuture<AppendEntriesReplyProto> appendEntriesAsync(RaftPeerId leaderId, long callId,
      TermIndex previous, ReferenceCountedObject<AppendEntriesRequestProto> requestRef) throws IOException {
    final AppendEntriesRequestProto proto = requestRef.get();
    final List<LogEntryProto> entries = proto.getEntriesList();
    final boolean isHeartbeat = entries.isEmpty();
    logAppendEntries(isHeartbeat, () -> getMemberId() + ": appendEntries* "
        + toAppendEntriesRequestString(proto, stateMachine::toStateMachineLogEntryString));
    RaftPeerId peerId = getId();
    //虚拟更随着节点处理
    if(getInfo().isFollower()&&peerId.isVirtual()) {
      String vnPeerId = proto.getVnPeerId();
      if (startComplete.get()) {
        if(!vnPeerId.isEmpty()&&!vnPeerId.equals(peerId.getId())){
          stopSeverState();
        }
      }else{
        if(!vnPeerId.isEmpty()&&!peerId.getId().equals(vnPeerId)){
          vnodeLease.extend();
        }
      }
    }


    //不可用状态终止日志追加。
    if(!startComplete.get()){
      updateLastRpcTime(FollowerState.UpdateType.NULL);

      return CompletableFuture.completedFuture(toAppendEntriesReplyProto(
        leaderId, getMemberId(), -1, RaftLog.INVALID_LOG_INDEX, RaftLog.INVALID_LOG_INDEX,
        AppendResult.UNAVAILABLE, callId, RaftLog.INVALID_LOG_INDEX, isHeartbeat, startComplete.get()));
    }

    final long leaderTerm = proto.getLeaderTerm();
    final long currentTerm;
    final long followerCommit = state.getLog().getLastCommittedIndex();
    final Optional<FollowerState> followerState;
    final Timekeeper.Context timer = raftServerMetrics.getFollowerAppendEntryTimer(isHeartbeat).time();
    final CompletableFuture<Void> future;
    synchronized (this) {
      // Check life cycle state again to avoid the PAUSING/PAUSED state.
      assertLifeCycleState(LifeCycle.States.STARTING_OR_RUNNING);
      currentTerm = state.getCurrentTerm();

      final boolean recognized = state.recognizeLeader(Op.APPEND_ENTRIES, leaderId, leaderTerm);
      if (!recognized) {
        return CompletableFuture.completedFuture(toAppendEntriesReplyProto(
            leaderId, getMemberId(), currentTerm, followerCommit, state.getNextIndex(),
            AppendResult.NOT_LEADER, callId, RaftLog.INVALID_LOG_INDEX, isHeartbeat, startComplete.get()));
      }
      try {
        future = changeToFollowerAndPersistMetadata(leaderTerm, true, "appendEntries");
      } catch (IOException e) {
        return JavaUtils.completeExceptionally(e);
      }
      state.setLeader(leaderId, "appendEntries");

      if (!proto.getInitializing() && lifeCycle.compareAndTransition(State.STARTING, State.RUNNING)) {
        role.startFollowerState(this, Op.APPEND_ENTRIES);
      }
      followerState = updateLastRpcTime(FollowerState.UpdateType.APPEND_START);

      // Check that the append entries are not inconsistent. There are 3
      // scenarios which can result in inconsistency:
      //      1. There is a snapshot installation in progress
      //      2. There is an overlap between the snapshot index and the entries
      //      3. There is a gap between the local log and the entries
      // In any of these scenarios, we should return an INCONSISTENCY reply
      // back to leader so that the leader can update this follower's next index.
      final long inconsistencyReplyNextIndex = checkInconsistentAppendEntries(previous, entries);
      if (inconsistencyReplyNextIndex > RaftLog.INVALID_LOG_INDEX) {
        final AppendEntriesReplyProto reply = toAppendEntriesReplyProto(
            leaderId, getMemberId(), currentTerm, followerCommit, inconsistencyReplyNextIndex,
            AppendResult.INCONSISTENCY, callId, RaftLog.INVALID_LOG_INDEX, isHeartbeat, startComplete.get());
        LOG.info("{}: appendEntries* reply {}", getMemberId(), toAppendEntriesReplyString(reply));
        followerState.ifPresent(fs -> fs.updateLastRpcTime(FollowerState.UpdateType.APPEND_COMPLETE));
        return future.thenApply(dummy -> reply);
      }

      state.updateConfiguration(entries);
    }
    future.join();

    final List<CompletableFuture<Long>> futures = entries.isEmpty() ? Collections.emptyList()
        : state.getLog().append(requestRef.delegate(entries));
    proto.getCommitInfosList().forEach(commitInfoCache::update);

    CodeInjectionForTesting.execute(LOG_SYNC, getId(), null);
    if (!isHeartbeat) {
      final long installedIndex = snapshotInstallationHandler.getInstalledIndex();
      if (installedIndex >= RaftLog.LEAST_VALID_LOG_INDEX) {
        LOG.info("{}: Follower has completed install the snapshot {}.", this, installedIndex);
        stateMachine.event().notifySnapshotInstalled(InstallSnapshotResult.SUCCESS, installedIndex, getPeer());
      }
    }

    final long commitIndex = effectiveCommitIndex(proto.getLeaderCommit(), previous, entries.size());
    final long matchIndex = isHeartbeat? RaftLog.INVALID_LOG_INDEX: entries.get(entries.size() - 1).getIndex();
    return JavaUtils.allOf(futures).whenCompleteAsync((r, t) -> {
      followerState.ifPresent(fs -> fs.updateLastRpcTime(FollowerState.UpdateType.APPEND_COMPLETE));
      timer.stop();
    }, getServerExecutor()).thenApply(v -> {
      final boolean updated = state.updateCommitIndex(commitIndex, currentTerm, false);
      if (updated) {
        updateCommitInfoCache();
      }
      final long nextIndex = isHeartbeat? state.getNextIndex(): matchIndex + 1;
      final AppendEntriesReplyProto reply = toAppendEntriesReplyProto(leaderId, getMemberId(),
          currentTerm, updated? commitIndex : state.getLog().getLastCommittedIndex(),
          nextIndex, AppendResult.SUCCESS, callId, matchIndex, isHeartbeat, startComplete.get());
      logAppendEntries(isHeartbeat, () -> getMemberId()
          + ": appendEntries* reply " + toAppendEntriesReplyString(reply));
      return reply;
    });
  }


  private long checkInconsistentAppendEntries(TermIndex previous, List<LogEntryProto> entries) {
    // Check if a snapshot installation through state machine is in progress.
    final long installSnapshot = snapshotInstallationHandler.getInProgressInstallSnapshotIndex();
    if (installSnapshot != RaftLog.INVALID_LOG_INDEX) {
      LOG.info("{}: Failed appendEntries as snapshot ({}) installation is in progress", getMemberId(), installSnapshot);
      return state.getNextIndex();
    }

    // Check that the first log entry is greater than the snapshot index in the latest snapshot and follower's last
    // committed index. If not, reply to the leader the new next index.
    if (entries != null && !entries.isEmpty()) {
      final long firstEntryIndex = entries.get(0).getIndex();
      final long snapshotIndex = state.getSnapshotIndex();
      final long commitIndex =  state.getLog().getLastCommittedIndex();
      final long nextIndex = Math.max(snapshotIndex, commitIndex);
      if (nextIndex > RaftLog.INVALID_LOG_INDEX && nextIndex >= firstEntryIndex) {
        LOG.info("{}: Failed appendEntries as the first entry (index {})" +
                " already exists (snapshotIndex: {}, commitIndex: {})",
            getMemberId(), firstEntryIndex, snapshotIndex, commitIndex);
        return nextIndex + 1;
      }
    }

    // Check if "previous" is contained in current state.
    if (previous != null && !state.containsTermIndex(previous)) {
      final long replyNextIndex = Math.min(state.getNextIndex(), previous.getIndex());
      LOG.info("{}: Failed appendEntries as previous log entry ({}) is not found", getMemberId(), previous);
      return replyNextIndex;
    }

    return RaftLog.INVALID_LOG_INDEX;
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(InstallSnapshotRequestProto request) throws IOException {
    if (!startComplete.get()) {
        throw new ServerNotReadyException(getMemberId() + ": The server role is not yet initialized.");
    }
    return snapshotInstallationHandler.installSnapshot(request);
  }

  boolean pause() {
    // TODO: should pause() be limited on only working for a follower?

    // Now the state of lifeCycle should be PAUSING, which will prevent future other operations.
    // Pause() should pause ongoing operations:
    //  a. call {@link StateMachine#pause()}.
    synchronized (this) {
      if (!lifeCycle.compareAndTransition(State.RUNNING, State.PAUSING)) {
        return false;
      }
      // TODO: any other operations that needs to be paused?
      stateMachine.pause();
      lifeCycle.compareAndTransition(State.PAUSING, State.PAUSED);
    }
    return true;
  }

  boolean resume() throws IOException {
    synchronized (this) {
      if (!lifeCycle.compareAndTransition(State.PAUSED, State.STARTING)) {
        return false;
      }
      // TODO: any other operations that needs to be resumed?
      try {
        stateMachine.reinitialize();
      } catch (IOException e) {
        LOG.warn("Failed to reinitialize statemachine: {}", stateMachine);
        lifeCycle.compareAndTransition(State.STARTING, State.EXCEPTION);
        throw e;
      }
      lifeCycle.compareAndTransition(State.STARTING, State.RUNNING);
    }
    return true;
  }

  /**
   * 该函数的功能是处理来自其他节点的发起领导选举请求，其核心逻辑如下：
   *   1. 解析请求中的领导者ID、组ID及日志信息；
   *   2. 若请求缺少领导者日志条目，则记录警告并返回拒绝响应；
   *   3. 校验当前节点生命周期状态为运行中且所属组一致；
   *   4. 再次确认生命周期状态为启动或运行，并验证领导者身份及任期是否匹配；
   *   5. 若当前角色不是跟随者（Follower）或日志落后于领导者，则拒绝请求；
   *   6. 否则转换角色为候选人（Candidate），触发选举流程并返回成功响应。
   *
   */
  @Override
  public StartLeaderElectionReplyProto startLeaderElection(StartLeaderElectionRequestProto request) throws IOException {
    final RaftRpcRequestProto r = request.getServerRequest();
    final RaftPeerId leaderId = RaftPeerId.valueOf(r.getRequestorId());
    final RaftGroupId leaderGroupId = ProtoUtils.toRaftGroupId(r.getRaftGroupId());
    CodeInjectionForTesting.execute(START_LEADER_ELECTION, getId(), leaderId, request);

    if (!request.hasLeaderLastEntry()) {
      // It should have a leaderLastEntry since there is a placeHolder entry.
      LOG.warn("{}: leaderLastEntry is missing in {}", getMemberId(), request);
      return toStartLeaderElectionReplyProto(leaderId, getMemberId(), false);
    }

    final TermIndex leaderLastEntry = TermIndex.valueOf(request.getLeaderLastEntry());
    LOG.info("{}: receive startLeaderElection from {} with lastEntry {}", getMemberId(), leaderId, leaderLastEntry);

    assertLifeCycleState(LifeCycle.States.RUNNING);
    assertGroup(getMemberId(), leaderId, leaderGroupId);

    synchronized (this) {
      // Check life cycle state again to avoid the PAUSING/PAUSED state.
      assertLifeCycleState(LifeCycle.States.STARTING_OR_RUNNING);
      final boolean recognized = state.recognizeLeader("startLeaderElection", leaderId, leaderLastEntry.getTerm());
      if (!recognized) {
        return toStartLeaderElectionReplyProto(leaderId, getMemberId(), false);
      }

      if (!getInfo().isFollower()) {
        LOG.warn("{} refused StartLeaderElectionRequest from {}, because role is:{}",
            getMemberId(), leaderId, role.getCurrentRole());
        return toStartLeaderElectionReplyProto(leaderId, getMemberId(), false);
      }

      if (ServerState.compareLog(state.getLastEntry(), leaderLastEntry) < 0) {
        LOG.warn("{} refused StartLeaderElectionRequest from {}, because lastEntry:{} less than leaderEntry:{}",
            getMemberId(), leaderId, leaderLastEntry, state.getLastEntry());
        return toStartLeaderElectionReplyProto(leaderId, getMemberId(), false);
      }

      changeToCandidate(true);
      return toStartLeaderElectionReplyProto(leaderId, getMemberId(), true);
    }
  }

  void submitUpdateCommitEvent() {
    role.getLeaderState().ifPresent(LeaderStateImpl::submitUpdateCommitEvent);
  }


  /**
   * 该函数的主要功能是处理状态机返回的结果，并据此更新待处理请求和重试缓存。
   * 具体逻辑如下：
   *   1. 获取或创建缓存条目：根据客户端调用ID从重试缓存中获取或创建一个条目。
   *   2. 日志警告与缓存刷新：
   *      - 如果当前节点是Leader且缓存条目已完成，输出警告。
   *      - 如果缓存条目标记为失败，则刷新该缓存条目。
   *   3. 处理状态机结果：
   *      - 当状态机异步操作完成时：
   *      - 移除事务管理器中的对应事务。
   *      - 构建响应对象，若无异常则设置成功状态及响应数据；否则封装为 StateMachineException。
   *      - 更新Leader的待处理请求并更新缓存结果。
   *
   */
  private CompletableFuture<Message> replyPendingRequest(
      ClientInvocationId invocationId, TermIndex termIndex, CompletableFuture<Message> stateMachineFuture) {
    // update the retry cache
    final CacheEntry cacheEntry = retryCache.getOrCreateEntry(invocationId);
    Objects.requireNonNull(cacheEntry , "cacheEntry == null");
    if (getInfo().isLeader() && cacheEntry.isCompletedNormally()) {
      LOG.warn("{} retry cache entry of leader should be pending: {}", this, cacheEntry);
    }
    if (cacheEntry.isFailed()) {
      retryCache.refreshEntry(new CacheEntry(cacheEntry.getKey()));
    }

    return stateMachineFuture.whenComplete((reply, exception) -> {
      getTransactionManager().remove(termIndex);
      final RaftClientReply.Builder b = newReplyBuilder(invocationId, termIndex.getIndex());
      final RaftClientReply r;
      if (exception == null) {
        r = b.setSuccess().setMessage(reply).build();
      } else {
        // the exception is coming from the state machine. wrap it into the
        // reply as a StateMachineException
        final StateMachineException e = new StateMachineException(getMemberId(), exception);
        r = b.setException(e).build();
      }

      // update pending request
      role.getLeaderState().ifPresent(leader -> leader.replyPendingRequest(termIndex, r));
      cacheEntry.updateResult(r);
    });
  }

  TransactionManager getTransactionManager() {
    return transactionManager;
  }

  @VisibleForTesting
  Map<TermIndex, MemoizedSupplier<TransactionContext>> getTransactionContextMapForTesting() {
    return getTransactionManager().getMap();
  }

  TransactionContext getTransactionContext(LogEntryProto entry, Boolean createNew) {
    if (!entry.hasStateMachineLogEntry()) {
      return null;
    }

    final TermIndex termIndex = TermIndex.valueOf(entry);
    final Optional<LeaderStateImpl> leader = getRole().getLeaderState();
    if (leader.isPresent()) {
      final TransactionContext context = leader.get().getTransactionContext(termIndex);
      if (context != null) {
        return context;
      }
    }

    if (!createNew) {
      return getTransactionManager().get(termIndex);
    }
    return getTransactionManager().computeIfAbsent(termIndex,
        // call startTransaction only once
        MemoizedSupplier.valueOf(() -> stateMachine.startTransaction(entry, getInfo().getCurrentRole())));
  }

  CompletableFuture<Message> applyLogToStateMachine(ReferenceCountedObject<LogEntryProto> nextRef)
      throws RaftLogIOException {
    LogEntryProto next = nextRef.get();
    CompletableFuture<Message> messageFuture = null;

    switch (next.getLogEntryBodyCase()) {
    case CONFIGURATIONENTRY:
      // the reply should have already been set. only need to record
      // the new conf in the metadata file and notify the StateMachine.
      state.writeRaftConfiguration(next);
      stateMachine.event().notifyConfigurationChanged(next.getTerm(), next.getIndex(),
          next.getConfigurationEntry());
      role.getLeaderState().ifPresent(leader -> leader.checkReady(next));
      break;
    case STATEMACHINELOGENTRY:
      TransactionContext trx = getTransactionContext(next, true);
      final ClientInvocationId invocationId = ClientInvocationId.valueOf(next.getStateMachineLogEntry());
      writeIndexCache.add(invocationId.getClientId(), ((TransactionContextImpl) trx).getLogIndexFuture());
      ((TransactionContextImpl) trx).setDelegatedRef(nextRef);
      try {
        // Let the StateMachine inject logic for committed transactions in sequential order.
        trx = stateMachine.applyTransactionSerial(trx);
        //异步应用事务到状态机，并通过 replyPendingRequest 处理响应和异常。
        final CompletableFuture<Message> stateMachineFuture = stateMachine.applyTransaction(trx);
        messageFuture = replyPendingRequest(invocationId, TermIndex.valueOf(next), stateMachineFuture);
      } catch (Exception e) {
        throw new RaftLogIOException(e);
      }
      break;
    case METADATAENTRY:
      break;
    default:
      throw new IllegalStateException("Unexpected LogEntryBodyCase " + next.getLogEntryBodyCase() + ", next=" + next);
    }

    if (next.getLogEntryBodyCase() != LogEntryProto.LogEntryBodyCase.STATEMACHINELOGENTRY) {
      stateMachine.event().notifyTermIndexUpdated(next.getTerm(), next.getIndex());
    }
    return messageFuture;
  }

  /**
   * The given log entry is being truncated.
   * Fail the corresponding client request, if there is any.
   *
   * @param logEntry the log entry being truncated
   */
  void notifyTruncatedLogEntry(LogEntryProto logEntry) {
    Optional.ofNullable(getState()).ifPresent(s -> s.truncate(logEntry.getIndex()));
    if (logEntry.hasStateMachineLogEntry()) {
      getTransactionManager().remove(TermIndex.valueOf(logEntry));

      final ClientInvocationId invocationId = ClientInvocationId.valueOf(logEntry.getStateMachineLogEntry());
      final CacheEntry cacheEntry = getRetryCache().getIfPresent(invocationId);
      if (cacheEntry != null) {
        cacheEntry.failWithReply(newReplyBuilder(invocationId, logEntry.getIndex())
            .setException(generateNotLeaderException())
            .build());
      }
    }
  }

  LeaderElectionMetrics getLeaderElectionMetrics() {
    return leaderElectionMetrics;
  }

  @Override
  public RaftServerMetricsImpl getRaftServerMetrics() {
    return raftServerMetrics;
  }

  void onGroupLeaderElected() {
    transferLeadership.complete(TransferLeadership.Result.SUCCESS);
  }

  boolean isRunning() {
    return startComplete.get()
      && lifeCycle.getCurrentState() == State.RUNNING;
  }
}
