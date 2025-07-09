package net.xdob.ratly.statemachine.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import net.xdob.ratly.client.impl.FastsImpl;
import net.xdob.ratly.proto.jdbc.WrapReplyProto;
import net.xdob.ratly.proto.jdbc.WrapRequestProto;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.protocol.*;
import net.xdob.ratly.security.crypto.factory.PasswordEncoderFactories;
import net.xdob.ratly.security.crypto.password.PasswordEncoder;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.storage.FileInfo;
import net.xdob.ratly.server.storage.RaftStorage;
import net.xdob.ratly.statemachine.RaftLogQuery;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.statemachine.StateMachineStorage;
import net.xdob.ratly.statemachine.TransactionContext;
import net.xdob.ratly.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CompoundStateMachine extends BaseStateMachine implements SMPluginContext {
  static final Logger LOG = LoggerFactory.getLogger(CompoundStateMachine.class);


  private final FastsImpl fasts = new FastsImpl();
  private final List<LeaderChangedListener> leaderChangedListeners = new ArrayList<>();

  private final PasswordEncoder passwordEncoder = PasswordEncoderFactories.createDelegatingPasswordEncoder();
  private ScheduledExecutorService scheduler;
  private final FileListStateMachineStorage storage = new FileListStateMachineStorage();
  private MemoizedSupplier<RaftLogQuery> logQuery;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
  private RaftPeerId peerId;

  private AutoCloseableLock readLock() {
    return AutoCloseableLock.acquire(lock.readLock());
  }

  private AutoCloseableLock writeLock() {
    return AutoCloseableLock.acquire(lock.writeLock());
  }

  private Map<String,SMPlugin> pluginMap = Maps.newConcurrentMap();

  public void addSMPlugin(SMPlugin plugin){
    plugin.setSMPluginContext(this);

    pluginMap.put(plugin.getId(), plugin);
  }

  public <T extends SMPlugin> Optional<T> getSMPlugin(Class<T> clazz){
    return (Optional<T>) pluginMap.values().stream()
        .filter(e->e.getClass().equals(clazz))
        .findFirst();
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
  }

  @Override
  public void initialize(RaftServer server, RaftGroupId groupId, RaftPeerId peerId,
                         RaftStorage raftStorage, MemoizedSupplier<RaftLogQuery> logQuery) throws IOException {
    super.initialize(server, groupId, peerId, raftStorage, logQuery);
    this.peerId = peerId;
    this.logQuery = logQuery;
    if(this.scheduler==null){
      this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }
    storage.init(raftStorage);
    for (SMPlugin plugin : pluginMap.values()) {
      plugin.initialize(server, groupId, peerId, raftStorage);
    }
  }


  @Override
  public void reinitialize() throws IOException {
    restoreFromSnapshot(getLatestSnapshot());
    for (SMPlugin plugin : pluginMap.values()) {
      plugin.reinitialize();
    }
  }

  public void addLeaderChangedListener(LeaderChangedListener listener) {
    leaderChangedListeners.add(listener);
  }

  public void removeLeaderChangedListener(LeaderChangedListener listener) {
    leaderChangedListeners.remove(listener);
  }

  private volatile CompletableFuture<RaftPeerId> leaderChangedFuture = new CompletableFuture<>();

  volatile boolean isLeader = false;

  public CompletableFuture<RaftPeerId> getLeaderChangedFuture() {
    return leaderChangedFuture;
  }

  @Override
  public void changeToCandidate(RaftGroupMemberId groupMemberId) {
    LOG.info("changeToCandidate: groupMemberId={}", groupMemberId);
    for (LeaderChangedListener listener : leaderChangedListeners) {
      try {
        listener.changeToCandidate(groupMemberId);
      }catch (Exception e){
        LOG.warn("{} listener.changeToCandidate error", listener, e);
      }
    }
  }

  @Override
  public void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId newLeaderId) {
    LOG.info("leaderChanged: groupMemberId={}, newLeaderId={}", groupMemberId.getPeerId(), newLeaderId);
    isLeader = groupMemberId.getPeerId().isOwner(newLeaderId);
    fireLeaderStateEvent(isLeader);
    leaderChangedFuture.complete(newLeaderId);
  }

  private void fireLeaderStateEvent(boolean isLeader) {
    LOG.info("fireLeaderStateEvent: isLeader={}", isLeader);
    for (LeaderChangedListener listener : leaderChangedListeners) {
      try {
        listener.notifyLeaderChanged(isLeader);
      }catch (Exception e){
        LOG.warn("{} listener.notifyLeaderChanged error", listener, e);
      }
    }
  }


  @Override
  public CompletableFuture<Message> query(Message request) {

    WrapReplyProto.Builder builder = WrapReplyProto.newBuilder();
    try(AutoCloseableLock readLock = readLock()) {
      WrapRequestProto wrapMsgProto = WrapRequestProto.parseFrom(request.getContent());
      String pluginId = wrapMsgProto.getType();
      SMPlugin smPlugin = pluginMap.get(pluginId);
      if(smPlugin!=null) {
        Object reply = smPlugin.query(Message.valueOf(wrapMsgProto.getMsg()));
        builder.setRelay(fasts.asByteString(reply));
      }else {
        throw new SQLException("plugin " + pluginId + " not find.");
      }
    } catch (SQLException e) {
      builder.setEx(fasts.asByteString(e));
    } catch (Exception e) {
      LOG.warn("", e);
    }
    return CompletableFuture.completedFuture(Message.valueOf(builder.build()));
  }




  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {

    WrapReplyProto.Builder builder = WrapReplyProto.newBuilder();
    ReferenceCountedObject<LogEntryProto> logEntryRef = trx.getLogEntryRef();
    try(AutoCloseableLock writeLock = writeLock()) {
      LogEntryProto entry = logEntryRef.retain();
      WrapRequestProto wrapMsgProto = WrapRequestProto.parseFrom(entry.getStateMachineLogEntry().getLogData());
      String pluginId = wrapMsgProto.getType();
      SMPlugin smPlugin = pluginMap.get(pluginId);
      if(smPlugin!=null) {
        //跳过已应用过的日志
        if(entry.getIndex()<smPlugin.getLastPluginAppliedIndex()){
          builder.setRelay(fasts.asByteString(null));
        }else {
          Object reply = smPlugin.applyTransaction(TermIndex.valueOf(entry.getTerm(), entry.getIndex()), wrapMsgProto.getMsg());
          updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
          builder.setRelay(fasts.asByteString(reply));
        }
      }else {
        throw new SQLException("plugin ["+pluginId+"] not find.");
      }
    } catch (InvalidProtocolBufferException e) {
      LOG.warn("", e);
    } catch (SQLException e) {
      builder.setEx(fasts.asByteString(e));
    } finally {
      logEntryRef.release();
    }
    return CompletableFuture.completedFuture(Message.valueOf(builder.build()));
  }

  private void restoreFromSnapshot(SnapshotInfo snapshot) throws IOException {
    if(snapshot==null){
      return;
    }
    LOG.info("restore from snapshot {} files={}",snapshot.getTermIndex(), snapshot.getFiles());
    try(AutoCloseableLock writeLock = writeLock()) {
      for (SMPlugin plugin : pluginMap.values()) {
        plugin.restoreFromSnapshot(snapshot);
      }
    }
  }


  @Override
  public long takeSnapshot() throws IOException {
    try(AutoCloseableLock readLock = readLock()) {
      Properties appliedIndexMap = new Properties();
      TermIndex last = getLastPluginAppliedTermIndex();
      List<FileInfo> infos = Lists.newArrayList();
      for (SMPlugin plugin : pluginMap.values()) {
        long appliedIndex = plugin.getLastPluginAppliedIndex();
        LOG.info("plugin {} index={}", plugin.getId(), plugin.getLastPluginAppliedIndex());
        if(appliedIndex>RaftLog.INVALID_LOG_INDEX){
          appliedIndexMap.setProperty(plugin.getId(), String.valueOf(appliedIndex));
        }
        List<FileInfo> fileInfos = plugin.takeSnapshot(storage, last);
        if(fileInfos!=null&&!fileInfos.isEmpty()){
          infos.addAll(fileInfos);
        }
      }
      if(!infos.isEmpty()) {
        storage.updateLatestSnapshot(new FileListSnapshotInfo(infos, last));
        return last.getIndex();
      }
      return super.takeSnapshot();
    }
  }

  /**
   * 获取最小插件事务阶段性索引
   * @return 最小插件事务阶段性索引
   */
  private TermIndex getLastPluginAppliedTermIndex() {
    TermIndex last = getLastAppliedTermIndex();
    Long lastPluginAppliedIndex = pluginMap.values().stream()
        .map(SMPlugin::getLastPluginAppliedIndex)
        .filter(e-> e > RaftLog.INVALID_LOG_INDEX)
        .min(Comparator.comparingLong(e->e))
        .orElse(RaftLog.INVALID_LOG_INDEX);
    if(lastPluginAppliedIndex>RaftLog.INVALID_LOG_INDEX){
      last = logQuery.get().getTermIndex(lastPluginAppliedIndex.longValue());
    }
    return last;
  }

  @Override
  public void close() throws IOException {
    super.close();
    if(scheduler!=null){
      this.scheduler.shutdown();
      this.scheduler = null;
    }
    for (SMPlugin plugin : pluginMap.values()) {
      try {
        plugin.close();
      } catch (IOException e) {
        LOG.warn("", e);
      }
    }
  }

  @Override
  public RaftPeerId getPeerId() {
    return peerId;
  }

  @Override
  public ScheduledExecutorService getScheduler() {
    return scheduler;
  }

  @Override
  public SerialSupport getFasts() {
    return fasts;
  }

  @Override
  public Object asObject(byte[] bytes) {
    return fasts.asObject(bytes);
  }

  @Override
  public Object asObject(ByteString byteString) {
    return fasts.asObject(byteString);
  }

  public ByteString getByteString(Object value) {
    return fasts.asByteString(value);
  }

  @Override
  public Object asObject(AbstractMessage msg) {
    return fasts.asObject(msg);
  }

  @Override
  public <T> T as(byte[] bytes) {
    return fasts.as(bytes);
  }

  @Override
  public <T> T as(ByteString byteString) {
    return fasts.as(byteString);
  }

  @Override
  public <T> T as(AbstractMessage msg) {
    return fasts.as(msg);
  }

  @Override
  public RaftLogQuery getRaftLogQuery() {
    return logQuery.get();
  }

  @Override
  public boolean isLeader() {
    return isLeader;
  }

  @Override
  public PasswordEncoder getPasswordEncoder() {
    return passwordEncoder;
  }


}
