package net.xdob.ratly.server.impl;

import net.xdob.onlooker.DefaultOnlookerClient;
import net.xdob.onlooker.MessageToken;
import net.xdob.onlooker.OnlookerClient;
import net.xdob.ratly.protocol.RaftGroupMemberId;
import net.xdob.ratly.server.config.RaftServerConfigKeys;
import net.xdob.ratly.server.raftlog.RaftLogIOException;
import net.xdob.ratly.server.storage.RaftStorage;
import net.xdob.ratly.server.storage.RaftStorageMetadata;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.proto.raft.RaftPeerRole;
import net.xdob.ratly.protocol.*;
import net.xdob.ratly.protocol.exceptions.StateMachineException;
import net.xdob.ratly.server.RaftConfiguration;
import net.xdob.ratly.server.impl.LeaderElection.Phase;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.LogProtoUtils;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.raftlog.memory.MemoryRaftLog;
import net.xdob.ratly.server.raftlog.segmented.SegmentedRaftLog;
import net.xdob.ratly.server.storage.*;
import net.xdob.ratly.proto.raft.InstallSnapshotRequestProto;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.server.util.SignHelper;
import net.xdob.ratly.statemachine.RaftLogQuery;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.statemachine.StateMachine;
import net.xdob.ratly.statemachine.TransactionContext;
import net.xdob.ratly.util.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static net.xdob.ratly.server.Division.LOG;

/**
 * Common states of a raft peer. Protected by RaftServer's lock.
 */
class ServerState implements RaftLogQuery {
  private final RaftGroupMemberId memberId;
  private final RaftServerImpl server;
  /** Raft log */
  private final MemoizedSupplier<RaftLog> log;
  /** Raft configuration */
  private final ConfigurationManager configurationManager;
  /** The thread that applies committed log entries to the state machine */
  private final MemoizedSupplier<StateMachineUpdater> stateMachineUpdater;
  /** local storage for log and snapshot */
  private final MemoizedCheckedSupplier<RaftStorageImpl, IOException> raftStorage;
  private final SnapshotManager snapshotManager;
  private final AtomicReference<Timestamp> lastNoLeaderTime;
  private final TimeDuration noLeaderTimeout;

  private final ReadRequests readRequests;

  /**
   * Latest term server has seen.
   * Initialized to 0 on first boot, increases monotonically.
   */
  private final AtomicLong currentTerm = new AtomicLong();
  /**
   * The server ID of the leader for this term. Null means either there is
   * no leader for this term yet or this server does not know who it is yet.
   */
  private final AtomicReference<RaftPeerId> leaderId = new AtomicReference<>();
  /**
   * Candidate that this peer granted vote for in current term (or null if none)
   */
  @SuppressWarnings({"squid:S3077"}) // Suppress volatile for generic type
  private volatile RaftPeerId votedFor;

  /**
   * Latest installed snapshot for this server. This maybe different than StateMachine's latest
   * snapshot. Once we successfully install a snapshot, the SM may not pick it up immediately.
   * Further, this will not get updated when SM does snapshots itself.
   */
  private final AtomicReference<TermIndex> latestInstalledSnapshot = new AtomicReference<>();

  private final OnlookerClient onlookerClient = new DefaultOnlookerClient();
  private SignHelper signHelper = new SignHelper();

  ServerState(RaftPeerId id, RaftGroup group, StateMachine stateMachine, RaftServerImpl server,
              StartupOption option, RaftProperties prop) {
    this.memberId = RaftGroupMemberId.valueOf(id, group.getGroupId());
    this.server = server;
    Collection<RaftPeer> followerPeers = group.getPeers().stream()
        .filter(peer -> peer.getStartupRole() == RaftPeerRole.FOLLOWER)
        .collect(Collectors.toList());
    Collection<RaftPeer> listenerPeers = group.getPeers().stream()
        .filter(peer -> peer.getStartupRole() == RaftPeerRole.LISTENER)
        .collect(Collectors.toList());
    final RaftConfigurationImpl initialConf = RaftConfigurationImpl.newBuilder()
        .setConf(followerPeers, listenerPeers)
        .build();
    configurationManager = new ConfigurationManager(id, initialConf);
    LOG.info("{}: {}", getMemberId(), configurationManager);

    final String storageDirName = group.getGroupId().getUuid().toString();
    this.raftStorage = MemoizedCheckedSupplier.valueOf(
        () -> StorageImplUtils.initRaftStorage(storageDirName, option, prop));

    this.snapshotManager = StorageImplUtils.newSnapshotManager(id, () -> getStorage().getStorageDir(),
        stateMachine.getStateMachineStorage());

    // On start the leader is null, start the clock now
    this.lastNoLeaderTime = new AtomicReference<>(Timestamp.currentTime());
    this.noLeaderTimeout = RaftServerConfigKeys.Notification.noLeaderTimeout(prop);

    final LongSupplier getSnapshotIndexFromStateMachine = () -> Optional.ofNullable(stateMachine.getLatestSnapshot())
        .map(SnapshotInfo::getIndex)
        .filter(i -> i >= 0)
        .orElse(RaftLog.INVALID_LOG_INDEX);
    this.log = JavaUtils.memoize(() -> initRaftLog(getSnapshotIndexFromStateMachine, prop));
    this.readRequests = new ReadRequests(prop, stateMachine);
    this.stateMachineUpdater = JavaUtils.memoize(() -> new StateMachineUpdater(
        stateMachine, server, this, getLog().getSnapshotIndex(), prop,
        this.readRequests.getAppliedIndexConsumer()));
  }

  void initialize(StateMachine stateMachine) throws IOException {
    // initialize raft storage
    final RaftStorageImpl storage = raftStorage.get();
    // read configuration from the storage
    Optional.ofNullable(storage.readRaftConfiguration()).ifPresent(this::setRaftConf);

    stateMachine.initialize(server.getRaftServer(), getMemberId().getGroupId(), storage,
        JavaUtils.memoize(()->this));

    // 在此步骤中，我们无法将日志条目应用于状态机，因为我们不知道本地日志是否已经提交。
    final RaftStorageMetadata metadata = log.get().loadMetadata();
    currentTerm.set(metadata.getTerm());
    votedFor = metadata.getVotedFor();
  }

  RaftGroupMemberId getMemberId() {
    return memberId;
  }

  void writeRaftConfiguration(LogEntryProto conf) {
    getStorage().writeRaftConfiguration(conf);
  }

  void start() {
    onlookerClient.start();
    // initialize stateMachineUpdater
    stateMachineUpdater.get().start();

  }

  private RaftLog initRaftLog(LongSupplier getSnapshotIndexFromStateMachine, RaftProperties prop) {
    try {
      return initRaftLog(getMemberId(), server, getStorage(), this::setRaftConf,
          getSnapshotIndexFromStateMachine, prop);
    } catch (IOException e) {
      throw new IllegalStateException(getMemberId() + ": Failed to initRaftLog.", e);
    }
  }

  @SuppressWarnings({"squid:S2095"}) // Suppress closeable  warning
  private static RaftLog initRaftLog(RaftGroupMemberId memberId, RaftServerImpl server, RaftStorage storage,
      Consumer<LogEntryProto> logConsumer, LongSupplier getSnapshotIndexFromStateMachine,
      RaftProperties prop) throws IOException {
    final RaftLog log;
    if (RaftServerConfigKeys.Log.useMemory(prop)) {
      log = new MemoryRaftLog(memberId, getSnapshotIndexFromStateMachine, prop);
    } else {
      log = SegmentedRaftLog.newBuilder()
          .setMemberId(memberId)
          .setServer(server)
          .setNotifyTruncatedLogEntry(server::notifyTruncatedLogEntry)
          .setGetTransactionContext(server::getTransactionContext)
          .setSubmitUpdateCommitEvent(server::submitUpdateCommitEvent)
          .setStorage(storage)
          .setSnapshotIndexSupplier(getSnapshotIndexFromStateMachine)
          .setProperties(prop)
          .build();
    }
    log.open(log.getSnapshotIndex(), logConsumer);
    return log;
  }

  RaftConfiguration getRaftConf() {
    return configurationManager.getCurrent();
  }

  RaftPeer getCurrentPeer() {
    return configurationManager.getCurrentPeer();
  }

  long getCurrentTerm() {
    return currentTerm.get();
  }

  boolean updateCurrentTerm(long newTerm) {
    final long current = currentTerm.getAndUpdate(curTerm -> Math.max(curTerm, newTerm));
    if (newTerm > current) {
      votedFor = null;
      setLeader(null, "updateCurrentTerm");
      return true;
    }
    return false;
  }

  RaftPeerId getLeaderId() {
    return leaderId.get();
  }

  /**
   * 成为候选人并开始领导选举
   */
  LeaderElection.ConfAndTerm initElection(Phase phase) throws IOException {
    setLeader(null, phase);
    final long term;
    if (phase == Phase.PRE_VOTE) {
      term = getCurrentTerm();
    } else if (phase == Phase.ELECTION) {
      term = currentTerm.incrementAndGet();
      votedFor = getMemberId().getPeerId();
      persistMetadata();
    } else {
      throw new IllegalArgumentException("Unexpected phase " + phase);
    }
    return new LeaderElection.ConfAndTerm(getRaftConf(), term);
  }

  void persistMetadata() throws IOException {
    getLog().persistMetadata(RaftStorageMetadata.valueOf(currentTerm.get(), votedFor));
  }

  RaftPeerId getVotedFor() {
    return votedFor;
  }

  /**
   * Vote for a candidate and update the local state.
   */
  void grantVote(RaftPeerId candidateId) {
    votedFor = candidateId;
    setLeader(null, "grantVote");
  }

  private final AtomicReference<MessageToken> tokenAtomicReference = new AtomicReference<>(null);

  void setLeader(RaftPeerId newLeaderId, Object op) {
    final RaftPeerId oldLeaderId = leaderId.getAndSet(newLeaderId);
    if (!Objects.equals(oldLeaderId, newLeaderId)) {
      final String suffix;
      if (newLeaderId == null) {
        // reset the time stamp when a null leader is assigned
        lastNoLeaderTime.set(Timestamp.currentTime());
        suffix = "";
      } else {
        final Timestamp previous = lastNoLeaderTime.getAndSet(null);
        suffix = ", leader elected after " + previous.elapsedTimeMs() + "ms";
        server.setFirstElection(op);
        server.getStateMachine().event().notifyLeaderChanged(getMemberId(), newLeaderId);
      }
      LOG.info("{}: change Leader from {} to {} at term {} for {}{}",
          getMemberId(), oldLeaderId, newLeaderId, getCurrentTerm(), op, suffix);
      if (newLeaderId != null) {
        server.onGroupLeaderElected();
        TermLeader termLeader = TermLeader.of(getCurrentTerm(), newLeaderId);
        MessageToken token = new MessageToken();
        token.setSigner(signHelper.getSigner());
        String message = termLeader.toString();
        token.setMessage(message);
        token.setSign(signHelper.sign(message));
        tokenAtomicReference.set(token);
      }
    }
  }

  void notifyTeamIndex(TermIndex termIndex){
    MessageToken token = tokenAtomicReference.get();
    token.setTeam(termIndex.getTerm());
    token.setIndex(termIndex.getIndex());
    onlookerClient.setMessage(memberId.getGroupId().toString(), token);
  }

  CompletableFuture<TermLeader> getLastLeaderTerm(int waitMS){
    final CompletableFuture<TermLeader> future = new CompletableFuture<>();
    onlookerClient.getMessageToken(memberId.getGroupId().toString(), waitMS)
        .whenComplete((r,ex)->{
          if(ex!=null){
            future.completeExceptionally(ex);
          }else{
            List<TermLeader> termLeaders = r.stream()
                .filter(e -> signHelper.verifySign(e.getMessage(), e.getSign()))
                .map(m -> {
                  TermLeader leader = TermLeader.parse(m.getMessage());
                  leader.setIndex(m.getIndex());
                  return leader;
                }).collect(Collectors.toList());
            long term = termLeaders.stream().mapToLong(TermLeader::getTerm).max().orElse(-1L);
            TermLeader termLeader = termLeaders.stream().filter(e->e.getTerm()== term)
                .max(Comparator.comparingLong(TermLeader::getIndex)).orElse(null);
            //LOG.info("tokens={}, termLeader={}", r, termLeader);
            future.complete(termLeader);
          }
        });
    return  future;
  }

  boolean shouldNotifyExtendedNoLeader() {
    return Optional.ofNullable(lastNoLeaderTime.get())
        .map(Timestamp::elapsedTime)
        .filter(t -> t.compareTo(noLeaderTimeout) > 0)
        .isPresent();
  }

  long getLastLeaderElapsedTimeMs() {
    return Optional.ofNullable(lastNoLeaderTime.get()).map(Timestamp::elapsedTimeMs).orElse(0L);
  }

  void becomeLeader() {
    setLeader(getMemberId().getPeerId(), "becomeLeader");
  }

  StateMachineUpdater getStateMachineUpdater() {
    if (!stateMachineUpdater.isInitialized()) {
      throw new IllegalStateException(getMemberId() + ": stateMachineUpdater is uninitialized.");
    }
    return stateMachineUpdater.get();
  }

  RaftLog getLog() {
    if (!log.isInitialized()) {
      throw new IllegalStateException(getMemberId() + ": log is uninitialized.");
    }
    return log.get();
  }

  TermIndex getLastEntry() {
    TermIndex lastEntry = getLog().getLastEntryTermIndex();
    if (lastEntry == null) {
      // lastEntry may need to be derived from snapshot
      SnapshotInfo snapshot = getLatestSnapshot();
      if (snapshot != null) {
        lastEntry = snapshot.getTermIndex();
      }
    }

    return lastEntry;
  }

  void appendLog(TransactionContext operation) throws StateMachineException {
    getLog().append(currentTerm.get(), operation);
    Objects.requireNonNull(operation.getLogEntryUnsafe(), "transaction-logEntry");
  }

  /** @return true iff the given peer id is recognized as the leader. */
  boolean recognizeLeader(Object op, RaftPeerId peerId, long peerTerm) {
    final long current = currentTerm.get();
    if (peerTerm < current) {
      LOG.warn("{}: Failed to recognize {} as leader for {} since peerTerm = {} < currentTerm = {}",
          getMemberId(), peerId, op, peerTerm, current);
      return false;
    }
    final RaftPeerId curLeaderId = getLeaderId();
    if (peerTerm == current && curLeaderId != null && !curLeaderId.equals(peerId)) {
      LOG.warn("{}: Failed to recognize {} as leader for {} since current leader is {} (peerTerm = currentTerm = {})",
          getMemberId(), peerId, op, curLeaderId, current);
      return false;
    }
    return true;
  }

  static int compareLog(TermIndex lastEntry, TermIndex candidateLastEntry) {
    if (lastEntry == null) {
      // If the lastEntry of candidate is null, the proto will transfer an empty TermIndexProto,
      // then term and index of candidateLastEntry will both be 0.
      // Besides, candidateLastEntry comes from proto now, it never be null.
      // But we still check candidateLastEntry == null here,
      // to avoid candidateLastEntry did not come from proto in future.
      if (candidateLastEntry == null ||
          (candidateLastEntry.getTerm() == 0 && candidateLastEntry.getIndex() == 0)) {
        return 0;
      }
      return -1;
    } else if (candidateLastEntry == null) {
      return 1;
    }

    return lastEntry.compareTo(candidateLastEntry);
  }

  @Override
  public String toString() {
    return getMemberId() + ":t" + currentTerm + ", leader=" + getLeaderId()
        + ", voted=" + votedFor + ", raftlog=" + log + ", conf=" + getRaftConf();
  }

  boolean isConfCommitted() {
    return getLog().getLastCommittedIndex() >= getRaftConf().getLogEntryIndex();
  }

  void setRaftConf(LogEntryProto entry) {
    if (entry.hasConfigurationEntry()) {
      setRaftConf(LogProtoUtils.toRaftConfiguration(entry));
    }
  }

  void setRaftConf(RaftConfiguration conf) {
    configurationManager.addConfiguration(conf);
    server.getServerRpc().addRaftPeers(conf.getAllPeers());
    final Collection<RaftPeer> listeners = conf.getAllPeers(RaftPeerRole.LISTENER);
    if (!listeners.isEmpty()) {
      server.getServerRpc().addRaftPeers(listeners);
    }
    LOG.info("{}: set configuration {}", getMemberId(), conf);
    LOG.trace("{}: {}", getMemberId(), configurationManager);
  }

  void truncate(long logIndex) {
    configurationManager.removeConfigurations(logIndex);
  }

  void updateConfiguration(List<LogEntryProto> entries) {
    if (entries != null && !entries.isEmpty()) {
      configurationManager.removeConfigurations(entries.get(0).getIndex());
      entries.forEach(this::setRaftConf);
    }
  }

  boolean updateCommitIndex(long majorityIndex, long curTerm, boolean isLeader) {
    if (getLog().updateCommitIndex(majorityIndex, curTerm, isLeader)) {
      getStateMachineUpdater().notifyUpdater();
      return true;
    }
    return false;
  }

  void notifyStateMachineUpdater() {
    getStateMachineUpdater().notifyUpdater();
  }

  void reloadStateMachine(TermIndex snapshotTermIndex) {
    getStateMachineUpdater().reloadStateMachine();

    getLog().onSnapshotInstalled(snapshotTermIndex.getIndex());
    latestInstalledSnapshot.set(snapshotTermIndex);
  }

  void close() {
    try {
      if (stateMachineUpdater.isInitialized()) {
        getStateMachineUpdater().stopAndJoin();
      }
    } catch (Throwable e) {
      if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
      }
      LOG.warn(getMemberId() + ": Failed to join " + getStateMachineUpdater(), e);
    }

    try {
      if (log.isInitialized()) {
        getLog().close();
      }
    } catch (Throwable e) {
      LOG.warn(getMemberId() + ": Failed to close raft log " + getLog(), e);
    }

    try {
      if (raftStorage.isInitialized()) {
        getStorage().close();
      }
    } catch (Throwable e) {
      LOG.warn(getMemberId() + ": Failed to close raft storage " + getStorage(), e);
    }
    try {
      onlookerClient.stop();
    }catch (Throwable e) {
      LOG.warn(getMemberId() + ": Failed to stop onlooker client", e);
    }
  }

  RaftStorageImpl getStorage() {
    if (!raftStorage.isInitialized()) {
      throw new IllegalStateException(getMemberId() + ": raftStorage is uninitialized.");
    }
    return raftStorage.getUnchecked();
  }

  void installSnapshot(InstallSnapshotRequestProto request) throws IOException {
    // TODO: verify that we need to install the snapshot
    StateMachine sm = server.getStateMachine();
    sm.pause(); // pause the SM to prepare for install snapshot
    snapshotManager.installSnapshot(request, sm);
  }

  private SnapshotInfo getLatestSnapshot() {
    return server.getStateMachine().getLatestSnapshot();
  }

  long getLatestInstalledSnapshotIndex() {
    final TermIndex ti = latestInstalledSnapshot.get();
    return ti != null? ti.getIndex(): RaftLog.INVALID_LOG_INDEX;
  }

  /**
   * The last index included in either the latestSnapshot or the latestInstalledSnapshot
   * @return the last snapshot index
   */
  long getSnapshotIndex() {
    final SnapshotInfo s = getLatestSnapshot();
    final long latestSnapshotIndex = s != null ? s.getIndex() : RaftLog.INVALID_LOG_INDEX;
    return Math.max(latestSnapshotIndex, getLatestInstalledSnapshotIndex());
  }

  long getNextIndex() {
    final long logNextIndex = getLog().getNextIndex();
    final long snapshotNextIndex = getLog().getSnapshotIndex() + 1;
    return Math.max(logNextIndex, snapshotNextIndex);
  }

  public long getLastAppliedIndex() {
    return getStateMachineUpdater().getStateMachineLastAppliedIndex();
  }

  @Override
  public TermIndex getTermIndex(long index) {
    return log.get().getTermIndex(index);
  }

  @Override
  public LogEntryProto get(long index) throws RaftLogIOException {
    return log.get().get(index);
  }

  @Override
  public LogEntryProto getStateMachineLog(long index) throws RaftLogIOException {
    RaftLog raftLog = log.get();
    LogEntryProto logEntryProto = raftLog.get(index);
    while(!logEntryProto.hasStateMachineLogEntry()){
      index-=1;
      if(index<0){
        logEntryProto = null;
        break;
      }
      logEntryProto = raftLog.get(index);
    }
    return logEntryProto;
  }

  boolean containsTermIndex(TermIndex ti) {
    Objects.requireNonNull(ti, "ti == null");

    if (Optional.ofNullable(latestInstalledSnapshot.get()).filter(ti::equals).isPresent()) {
      return true;
    }
    if (Optional.ofNullable(getLatestSnapshot()).map(SnapshotInfo::getTermIndex).filter(ti::equals).isPresent()) {
      return true;
    }
    return getLog().contains(ti);
  }

  ReadRequests getReadRequests() {
    return readRequests;
  }

}
