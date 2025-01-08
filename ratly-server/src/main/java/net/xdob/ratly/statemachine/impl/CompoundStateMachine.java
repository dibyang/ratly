package net.xdob.ratly.statemachine.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import net.xdob.ratly.fasts.serialization.FSTConfiguration;
import net.xdob.ratly.proto.jdbc.QueryReplyProto;
import net.xdob.ratly.proto.jdbc.SQLExceptionProto;
import net.xdob.ratly.proto.jdbc.UpdateReplyProto;
import net.xdob.ratly.proto.jdbc.WrapMsgProto;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftGroupMemberId;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.storage.FileInfo;
import net.xdob.ratly.server.storage.RaftStorage;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.statemachine.StateMachineStorage;
import net.xdob.ratly.statemachine.TransactionContext;
import net.xdob.ratly.util.AutoCloseableLock;
import net.xdob.ratly.util.ReferenceCountedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

public class CompoundStateMachine extends BaseStateMachine implements SMPluginContext {
  static final Logger LOG = LoggerFactory.getLogger(CompoundStateMachine.class);



  private final FSTConfiguration fasts = FSTConfiguration.createDefaultConfiguration();
  private final List<LeaderChangedListener> leaderChangedListeners = new ArrayList<>();


  private ScheduledExecutorService scheduler;
  private final FileListStateMachineStorage storage = new FileListStateMachineStorage();
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  private AutoCloseableLock readLock() {
    return AutoCloseableLock.acquire(lock.readLock());
  }

  private AutoCloseableLock writeLock() {
    return AutoCloseableLock.acquire(lock.writeLock());
  }

  private Map<String,SMPlugin> pluginMap = Maps.newConcurrentMap();

  public void addSMPlugin(SMPlugin plugin){
    pluginMap.put(plugin.getId(), plugin);
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return storage;
  }

  @Override
  public void initialize(RaftServer server, RaftGroupId groupId,
                         RaftStorage raftStorage) throws IOException {
    super.initialize(server, groupId, raftStorage);
    storage.init(raftStorage);
    this.scheduler = Executors.newSingleThreadScheduledExecutor();
    for (SMPlugin plugin : pluginMap.values()) {
      plugin.initialize(server, groupId, raftStorage, this);
    }
    restoreFromSnapshot(getLatestSnapshot());
  }


  @Override
  public void reinitialize() throws IOException {
    restoreFromSnapshot(getLatestSnapshot());
  }

  public void addLeaderChangedListener(LeaderChangedListener listener) {
    leaderChangedListeners.add(listener);
  }

  public void removeLeaderChangedListener(LeaderChangedListener listener) {
    leaderChangedListeners.remove(listener);
  }

  volatile boolean isLeader = false;

  @Override
  public void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId newLeaderId) {
    isLeader = groupMemberId.getPeerId().equals(newLeaderId);
    if(!isLeader) {
      fireLeaderStateEvent(l -> l.notifyLeaderChanged(false));
    }
  }

  private void fireLeaderStateEvent(Consumer<LeaderChangedListener> consumer) {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.submit(()->{
      for (LeaderChangedListener listener : leaderChangedListeners) {
        try {
          consumer.accept(listener);
        }catch (Exception e){
          LOG.warn("", e);
        }
      }
      if(!executorService.isShutdown()){
        executorService.shutdown();
      }
    });
    scheduler.schedule(()->{
      if(!executorService.isShutdown()){
        LOG.warn("fireLeaderStateEvent use time > 5s.");
        executorService.shutdown();
      }
    }, 5, TimeUnit.SECONDS);
  }

  @Override
  public void notifyLeaderReady() {
    fireLeaderStateEvent(l -> l.notifyLeaderChanged(true));
  }




//  /**
//   * Leader首次准备完成
//   */
//  public CompletableFuture<Boolean> getFirstLeaderReady() {
//    return firstLeaderReady;
//  }

  @Override
  public CompletableFuture<Message> query(Message request) {

    QueryReplyProto.Builder builder = QueryReplyProto.newBuilder();
    try(AutoCloseableLock readLock = readLock()) {
      WrapMsgProto wrapMsgProto = WrapMsgProto.parseFrom(request.getContent());
      SMPlugin smPlugin = pluginMap.get(wrapMsgProto.getType());
      if(smPlugin!=null) {
        return smPlugin.query(Message.valueOf(wrapMsgProto.getMsg()));
      }
      throw new SQLException("plugin {} not find.");
    } catch (SQLException e) {
      builder.setEx(getSqlExceptionProto(e));
    } catch (Exception e) {
      LOG.warn("", e);
    }
    return CompletableFuture.completedFuture(Message.valueOf(builder.build()));
  }

  public SQLExceptionProto getSqlExceptionProto(SQLException e) {
    SQLExceptionProto.Builder builder = SQLExceptionProto.newBuilder();
    if(e.getMessage()!=null){
      builder.setReason(e.getMessage());
    }
    if(e.getSQLState()!=null){
      builder.setState(e.getSQLState());
    }
    builder.setErrorCode(e.getErrorCode())
        .setStacktrace(getByteString(e.getStackTrace()));
    return builder.build();
  }


  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {

    UpdateReplyProto.Builder builder = UpdateReplyProto.newBuilder();
    ReferenceCountedObject<LogEntryProto> logEntryRef = trx.getLogEntryRef();
    try(AutoCloseableLock writeLock = writeLock()) {
      LogEntryProto entry = logEntryRef.get();
      WrapMsgProto wrapMsgProto = WrapMsgProto.parseFrom(entry.getStateMachineLogEntry().getLogData());
      SMPlugin smPlugin = pluginMap.get(wrapMsgProto.getType());
      if(smPlugin!=null) {
        return smPlugin.applyTransaction(TermIndex.valueOf(entry.getTerm(), entry.getIndex()), wrapMsgProto.getMsg());
      }
      throw new SQLException("plugin {} not find.");

    } catch (InvalidProtocolBufferException e) {
      LOG.warn("", e);
    } catch (SQLException e) {
      builder.setEx(getSqlExceptionProto(e));
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
      TermIndex last = getLastAppliedTermIndex();
      List<FileInfo> infos = Lists.newArrayList();
      for (SMPlugin plugin : pluginMap.values()) {
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

  @Override
  public void close() throws IOException {
    super.close();
    for (SMPlugin plugin : pluginMap.values()) {
      try {
        plugin.close();
      } catch (IOException e) {
        LOG.warn("", e);
      }
    }
  }


  @Override
  public boolean updateLastAppliedTermIndex2(long term, long index) {
    return super.updateLastAppliedTermIndex(term, index);
  }

  @Override
  public boolean updateLastAppliedTermIndex2(TermIndex termIndex) {
    return super.updateLastAppliedTermIndex(termIndex);
  }

  @Override
  public ScheduledExecutorService getScheduler() {
    return scheduler;
  }

  @Override
  public FSTConfiguration getFasts() {
    return fasts;
  }

  @Override
  public Object asObject(byte[] bytes) {
    if(bytes==null||bytes.length==0){
      return null;
    }
    return fasts.asObject(bytes);
  }

  @Override
  public Object asObject(ByteString byteString) {
    if(byteString==null){
      return null;
    }
    return asObject(byteString.toByteArray());
  }

  public ByteString getByteString(Object value) {
    return ByteString.copyFrom(fasts.asByteArray(value));
  }

  public static void main(String[] args) {
    FSTConfiguration fasts = FSTConfiguration.createDefaultConfiguration();
    byte[] bytes = fasts.asByteArray(null);
    System.out.println("bytes = " + bytes.length);
    byte[] bytes1 = ByteString.empty().toByteArray();
    System.out.println("bytes1 = " + bytes1.length);
    Object object = fasts.asObject(bytes1);
    System.out.println("object = " + object);
  }
}
