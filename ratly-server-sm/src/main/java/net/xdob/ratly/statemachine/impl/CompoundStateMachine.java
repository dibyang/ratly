package net.xdob.ratly.statemachine.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.io.Digest;
import net.xdob.ratly.proto.base.Throwable2Proto;
import net.xdob.ratly.proto.sm.ErrorProto;
import net.xdob.ratly.proto.sm.WrapReplyProto;
import net.xdob.ratly.proto.sm.WrapRequestProto;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.protocol.*;
import net.xdob.ratly.security.crypto.factory.PasswordEncoderFactories;
import net.xdob.ratly.security.crypto.password.PasswordEncoder;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.exception.SuccessApplied;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.storage.FileInfo;
import net.xdob.ratly.server.storage.RaftStorage;
import net.xdob.ratly.statemachine.ServerStateSupport;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.statemachine.StateMachineStorage;
import net.xdob.ratly.statemachine.TransactionContext;
import net.xdob.ratly.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class CompoundStateMachine extends BaseStateMachine implements SMPluginContext {
  static final Logger LOG = LoggerFactory.getLogger(CompoundStateMachine.class);



  private final List<LeaderChangedListener> leaderChangedListeners = new ArrayList<>();

  private final PasswordEncoder passwordEncoder = PasswordEncoderFactories.createDelegatingPasswordEncoder();
  private ScheduledExecutorService scheduler;
  private final FileListStateMachineStorage storage = new FileListStateMachineStorage();
  private MemoizedSupplier<ServerStateSupport> serverStateSupport;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
  private RaftPeerId peerId;
	private final AtomicReference<RaftPeerId> leaderId = new AtomicReference<>(RaftPeerId.EMPTY);
	private RaftClient raftClient;

	private volatile CompletableFuture<RaftPeerId> leaderChangedFuture = new CompletableFuture<>();

	volatile boolean isLeader = false;

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
                         RaftStorage raftStorage, MemoizedSupplier<ServerStateSupport> logQuery) throws IOException {
    super.initialize(server, groupId, peerId, raftStorage, logQuery);
    this.peerId = peerId;
		// 创建RaftClient
		RaftGroup group = null;
		for (RaftGroup serverGroup : server.getGroups()) {
			if(serverGroup.getGroupId().equals(groupId)){
				group = serverGroup;
				break;
			}
		}
		this.raftClient = RaftClient.newBuilder()
				.setRaftGroup(group)
				.setProperties(server.getProperties())
				.build();
    this.serverStateSupport = logQuery;
    if(this.scheduler==null){
      this.scheduler = Executors.newScheduledThreadPool(6);
    }
    storage.init(raftStorage);
    for (SMPlugin plugin : pluginMap.values()) {
      plugin.initialize(server, groupId, peerId, raftStorage);
    }
		LOG.info("{} initialize", getPeerId());
		restoreFromSnapshot(getLatestSnapshot());
  }

	public RaftClient getRaftClient() {
		return raftClient;
	}

	@Override
  public void reinitialize() throws IOException {
		LOG.info("{} reinitialize", getPeerId());
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



  public CompletableFuture<RaftPeerId> getLeaderChangedFuture() {
    return leaderChangedFuture;
  }



  @Override
  public void changeToCandidate(RaftGroupMemberId groupMemberId) {
		leaderId.set(RaftPeerId.EMPTY);
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
		leaderId.set(newLeaderId);
		isLeader = groupMemberId.getPeerId().isOwner(newLeaderId);
    scheduler.submit(()->fireLeaderStateEvent(isLeader));
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
	public CompletableFuture<Message> admin(Message request) {
		WrapReplyProto.Builder response = WrapReplyProto.newBuilder();
		try{
			WrapRequestProto requestProto = WrapRequestProto.parseFrom(request.getContent());
			String pluginId = requestProto.getType();
			SMPlugin smPlugin = pluginMap.get(pluginId);
			if(smPlugin!=null) {
				smPlugin.admin(requestProto, response);
			}else {
				throw new IOException("plugin not found:"+ pluginId);
			}
		} catch (Exception e) {
			response.setEx(Proto2Util.toThrowable2Proto(e));
			LOG.warn("", e);
		}
		return CompletableFuture.completedFuture(Message.valueOf(response.build()));
	}

	@Override
  public CompletableFuture<Message> query(Message request) {

    WrapReplyProto.Builder response = WrapReplyProto.newBuilder();
    try(AutoCloseableLock readLock = readLock()) {
      WrapRequestProto requestProto = WrapRequestProto.parseFrom(request.getContent());
      String pluginId = requestProto.getType();
      SMPlugin smPlugin = pluginMap.get(pluginId);
      if(smPlugin!=null) {
        smPlugin.query(requestProto, response);
      }else {
        throw new IOException("plugin not found:"+ pluginId);
      }
    } catch (Exception e) {
			response.setEx(Proto2Util.toThrowable2Proto(e));
      LOG.warn("", e);
    }
    return CompletableFuture.completedFuture(Message.valueOf(response.build()));
  }

	@Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {

    WrapReplyProto.Builder response = WrapReplyProto.newBuilder();
    ReferenceCountedObject<LogEntryProto> logEntryRef = trx.getLogEntryRef();
		LogEntryProto entry = logEntryRef.retain();
		try(AutoCloseableLock writeLock = writeLock()) {
      if(entry.getIndex()>getLastAppliedTermIndex().getIndex()){
        WrapRequestProto wrapMsgProto = WrapRequestProto.parseFrom(entry.getStateMachineLogEntry().getLogData());
        String pluginId = wrapMsgProto.getType();
        SMPlugin smPlugin = pluginMap.get(pluginId);
        if(smPlugin!=null) {
					smPlugin.applyTransaction(TermIndex.valueOf(entry.getTerm(), entry.getIndex()), wrapMsgProto, response);
					updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
        }else {
          throw new IOException("plugin not found:"+ pluginId);
        }
      }else{
				LOG.warn("log skip index:{}", entry.getIndex());
      }
    } catch (Exception e) {
			response.setEx(Proto2Util.toThrowable2Proto(e));
      LOG.warn("", e);
			if(e instanceof SuccessApplied){
				updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
			}
    } finally {
      logEntryRef.release();
    }
    return CompletableFuture.completedFuture(Message.valueOf(response.build()));
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
			setLastAppliedTermIndex(snapshot.getTermIndex());
    }
		LOG.info("restore success from snapshot {} ",snapshot.getTermIndex());
	}


  @Override
  public long takeSnapshot() throws IOException {
    TermIndex last = null;
		List<FileInfo> infos = Lists.newArrayList();
		Map<String, List<FileInfo>> map = Maps.newHashMap();
		try{
			try(AutoCloseableLock readLock = readLock()) {
				last = this.getLastTxAppliedTermIndex();
				if(last.getIndex()<0){
					return last.getIndex();
				}
				for (SMPlugin plugin : pluginMap.values()) {
					LOG.info("takeSnapshot plugin {} index={}", plugin.getId(), last.getIndex());
					List<FileInfo> fileInfos = plugin.takeSnapshot(storage, last);
					map.put(plugin.getId(), fileInfos);
				}
			}
			for (SMPlugin plugin : pluginMap.values()) {
				LOG.info("finishSnapshot plugin {} index={}", plugin.getId(), last.getIndex());
				List<FileInfo> fileInfos = map.get(plugin.getId());
				if(fileInfos!=null&&!fileInfos.isEmpty()) {
					plugin.finishSnapshot(storage, last, fileInfos);
					if (!fileInfos.isEmpty()) {
						infos.addAll(fileInfos);
					}
				}
			}
			if(!infos.isEmpty()) {
				File sumFile = storage.getSnapshotSumFile(last.getTerm(), last.getIndex());
				StringBuilder lines = new StringBuilder();
				infos.forEach(info->{
					lines.append(info.getPath().getFileName()).append("\n");
				});
				try (AtomicFileOutputStream afos = new AtomicFileOutputStream(sumFile)) {
					afos.write(lines.toString().getBytes(StandardCharsets.UTF_8));
				}
				Digest digest = MD5FileUtil.computeAndSaveDigestForFile(sumFile);
				infos.add(new FileInfo(sumFile.toPath(), digest, FileListSnapshotInfo.SUM));
				storage.updateLatestSnapshot(new FileListSnapshotInfo(infos, last));
				return last.getIndex();
			}
		}catch (Exception e){
			if(last.getIndex()>RaftLog.INVALID_LOG_INDEX){
				storage.cleanupSnapshot(last.getTerm(), last.getIndex());
			}
			throw e;
		}
    return super.takeSnapshot();
  }

  /**
   * 获取最新事务阶段性索引
   * @return 获取最新事务阶段性索引
   */
  @Override
  public TermIndex getLastTxAppliedTermIndex() {
		//初始值表示不能做快照
    TermIndex last = TermIndex.INITIAL_VALUE;
		long tx = pluginMap.values().stream()
				.map(SMPlugin::getFirstTx)
				.filter(e -> e > RaftLog.INVALID_LOG_INDEX)
				.min(Long::compareTo)
				.orElse(RaftLog.INVALID_LOG_INDEX);
		List<Long> indexList = pluginMap.values().stream()
				.flatMap(e -> e.getLastEndedTxIndexList().stream())
				.filter(e -> e > RaftLog.INVALID_LOG_INDEX)
				.sorted()
				.collect(Collectors.toList());

		long lastEndedTxIndex = indexList.stream()
				.max(Long::compareTo)
				.orElse(RaftLog.INVALID_LOG_INDEX);
		//有未完成的事务,并且后面有已提交事务
		if(tx>0&&lastEndedTxIndex>tx){
			lastEndedTxIndex = RaftLog.INVALID_LOG_INDEX;
		}
    if(lastEndedTxIndex>RaftLog.INVALID_LOG_INDEX){
      last = serverStateSupport.get().getTermIndex(lastEndedTxIndex);
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
	public RaftPeerId getLeaderId() {
		return this.leaderId.get();
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
  public ServerStateSupport getServerStateSupport() {
    return serverStateSupport.get();
  }


  @Override
  public PasswordEncoder getPasswordEncoder() {
    return passwordEncoder;
  }

  @Override
  public void stopServerState() {
    getServerStateSupport().stopServerState();
  }


}
