package net.xdob.ratly.statemachine.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import net.xdob.ratly.client.impl.FastsImpl;
import net.xdob.ratly.fasts.serialization.FSTConfiguration;
import net.xdob.ratly.io.MD5Hash;
import net.xdob.ratly.proto.jdbc.QueryReplyProto;
import net.xdob.ratly.proto.jdbc.SQLExceptionProto;
import net.xdob.ratly.proto.jdbc.UpdateReplyProto;
import net.xdob.ratly.proto.jdbc.WrapMsgProto;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.protocol.*;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

public class CompoundStateMachine extends BaseStateMachine implements SMPluginContext {
  static final Logger LOG = LoggerFactory.getLogger(CompoundStateMachine.class);
  public static final String INDEX4PLUGIN = "index4plugin";


  private final FastsImpl fasts = new FastsImpl();
  private final List<LeaderChangedListener> leaderChangedListeners = new ArrayList<>();


  private ScheduledExecutorService scheduler;
  private final FileListStateMachineStorage storage = new FileListStateMachineStorage();
  private MemoizedSupplier<RaftLogQuery> logQuery;
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
                         RaftStorage raftStorage, MemoizedSupplier<RaftLogQuery> logQuery) throws IOException {
    super.initialize(server, groupId, raftStorage, logQuery);
    this.logQuery = logQuery;
    storage.init(raftStorage);
    this.scheduler = Executors.newSingleThreadScheduledExecutor();
    for (SMPlugin plugin : pluginMap.values()) {
      plugin.initialize(server, groupId, raftStorage, this);
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
      LogEntryProto entry = logEntryRef.retain();
      WrapMsgProto wrapMsgProto = WrapMsgProto.parseFrom(entry.getStateMachineLogEntry().getLogData());
      SMPlugin smPlugin = pluginMap.get(wrapMsgProto.getType());
      if(smPlugin!=null) {
        //跳过已应用过的日志
        if(entry.getIndex()<smPlugin.getLastPluginAppliedIndex()){
          return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Message> future = smPlugin.applyTransaction(TermIndex.valueOf(entry.getTerm(), entry.getIndex()), wrapMsgProto.getMsg());
        updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
        return future;
      }
      throw new SQLException("plugin {} not find.");

    } catch (InvalidProtocolBufferException e) {
      LOG.warn("", e);
    } catch (SQLException e) {
      builder.setEx(getSqlExceptionProto(e));
    }finally {
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
      Properties appliedIndexMap = new Properties();;
      FileInfo fileInfo = snapshot.getFiles(INDEX4PLUGIN).stream().findFirst().orElse(null);
      if(fileInfo!=null){
        File snapshotFile = fileInfo.getPath().toFile();
        final String snapshotFileName = snapshotFile.getPath();
        LOG.info("restore index4plugin snapshot from {}", snapshotFileName);
        final MD5Hash md5 = MD5FileUtil.computeMd5ForFile(snapshotFile);
        if (md5.equals(fileInfo.getFileDigest())) {
          try(BufferedReader br = new BufferedReader(new InputStreamReader(
              FileUtils.newInputStream(snapshotFile), StandardCharsets.UTF_8))){
            appliedIndexMap.load(br);
          }
        }
      }
      for (SMPlugin plugin : pluginMap.values()) {
        long newIndex = Optional.ofNullable(appliedIndexMap.getProperty(plugin.getId()))
            .map(Long::parseLong).orElse(RaftLog.INVALID_LOG_INDEX);
        if(newIndex>RaftLog.INVALID_LOG_INDEX){
          plugin.updateAppliedIndexToMax(newIndex);
        }
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
        final File snapshotFile =  storage.getSnapshotFile(INDEX4PLUGIN, last.getTerm(), last.getIndex());
        LOG.info("Taking a index4plugin snapshot to file {}", snapshotFile);
        try(BufferedWriter out = new BufferedWriter(
            new OutputStreamWriter(new AtomicFileOutputStream(snapshotFile), StandardCharsets.UTF_8))){
          appliedIndexMap.store(out, "last ="+last);
        }
        final MD5Hash md5 = MD5FileUtil.computeAndSaveMd5ForFile(snapshotFile);
        LOG.info("index4plugin md5={}", md5);
        final FileInfo info = new FileInfo(snapshotFile.toPath(), md5);
        infos.add(info);
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
    for (SMPlugin plugin : pluginMap.values()) {
      try {
        plugin.close();
      } catch (IOException e) {
        LOG.warn("", e);
      }
    }
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

}
