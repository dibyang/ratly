package net.xdob.ratly.server.impl;

import com.google.common.collect.Lists;
import net.xdob.ratly.RaftConfigKeys;
import net.xdob.ratly.protocol.*;
import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.datastream.SupportedDataStreamType;
import net.xdob.ratly.proto.raft.ReadIndexRequestProto;
import net.xdob.ratly.proto.raft.ReadIndexReplyProto;
import net.xdob.ratly.proto.raft.AppendEntriesReplyProto;
import net.xdob.ratly.proto.raft.AppendEntriesRequestProto;
import net.xdob.ratly.proto.raft.InstallSnapshotReplyProto;
import net.xdob.ratly.proto.raft.InstallSnapshotRequestProto;
import net.xdob.ratly.proto.raft.RaftRpcRequestProto;
import net.xdob.ratly.proto.raft.RequestVoteReplyProto;
import net.xdob.ratly.proto.raft.RequestVoteRequestProto;
import net.xdob.ratly.proto.raft.StartLeaderElectionReplyProto;
import net.xdob.ratly.proto.raft.StartLeaderElectionRequestProto;
import net.xdob.ratly.protocol.exceptions.AlreadyClosedException;
import net.xdob.ratly.protocol.exceptions.AlreadyExistsException;
import net.xdob.ratly.protocol.exceptions.BeanNotFindException;
import net.xdob.ratly.protocol.exceptions.GroupMismatchException;
import net.xdob.ratly.rpc.RpcType;
import net.xdob.ratly.server.*;
import net.xdob.ratly.server.storage.StartupOption;
import net.xdob.ratly.util.Concurrents3;
import net.xdob.ratly.util.JvmPauseMonitor;
import net.xdob.ratly.server.config.RaftServerConfigKeys;
import net.xdob.ratly.statemachine.StateMachine;
import net.xdob.ratly.util.IOUtils;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.LifeCycle;
import net.xdob.ratly.util.MemoizedSupplier;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.ProtoUtils;
import net.xdob.ratly.util.ReferenceCountedObject;
import net.xdob.ratly.util.TimeDuration;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;


class RaftServerProxy implements RaftServer {
  private final List<BeanFinder> beanFinders = Lists.newCopyOnWriteArrayList();

  @Override
  public <T> Optional<T> getBean(BeanTarget<T> target) {
    for (BeanFinder beanFinder : beanFinders) {
      T bean = beanFinder.getBean(target);
      if(bean!=null){
        return Optional.ofNullable(bean);
      }
    }
    return Optional.empty();
  }

  @Override
  public void addBeanFinder(BeanFinder beanFinder) {
    beanFinders.add(beanFinder);
  }

  @Override
  public void removeBeanFinder(BeanFinder beanFinder) {
    beanFinders.remove(beanFinder);
  }

  /**
   * 一个映射：{@link RaftGroupId} -> {@link RaftServerImpl} 的Future对象。
   * <p>
   * 该映射在进行修改以及批量的 {@link #getGroupIds()}/{@link #getAll()} 方法时是同步的，
   * 但（非批量的）{@link #get(RaftGroupId)} 和 {@link #containsGroup(RaftGroupId)} 方法则不是。
   * 非批量方法的线程安全性和原子性保证由 {@link ConcurrentMap} 提供。
   */

  class ImplMap implements Closeable {
    private final ConcurrentMap<RaftGroupId, CompletableFuture<RaftServerImpl>> map = new ConcurrentHashMap<>();
    private boolean isClosed = false;

    synchronized CompletableFuture<RaftServerImpl> addNew(RaftGroup group, StartupOption option) {
      if (isClosed) {
        return JavaUtils.completeExceptionally(new AlreadyClosedException(
            getId() + ": Failed to add " + group + " since the server is already closed"));
      }
      if (containsGroup(group.getGroupId())) {
        return JavaUtils.completeExceptionally(new AlreadyExistsException(
            getId() + ": Failed to add " + group + " since the group already exists in the map."));
      }
      final RaftGroupId groupId = group.getGroupId();
      final CompletableFuture<RaftServerImpl> newImpl = newRaftServerImpl(group, option);
      final CompletableFuture<RaftServerImpl> previous = map.put(groupId, newImpl);
      Preconditions.assertNull(previous, "previous");
      LOG.info("{}: addNew {} returns {}", getId(), group, toString(groupId, newImpl));
      return newImpl;
    }

    synchronized CompletableFuture<RaftServerImpl> remove(RaftGroupId groupId) {
      if (!map.containsKey(groupId)) {
        LOG.warn("{}: does not contain group: {}", getId(), groupId);
        return null;
      }

      final CompletableFuture<RaftServerImpl> future = map.remove(groupId);
      LOG.info("{}: remove {}", getId(), toString(groupId, future));
      return future;
    }

    @Override
    public synchronized void close() {
      if (isClosed) {
        LOG.info("{} is already closed.", getId());
        return;
      }
      isClosed = true;
      Concurrents3.parallelForEachAsync(map.entrySet(),
          entry -> close(entry.getKey(), entry.getValue()),
          executor.get());
    }

    private void close(RaftGroupId groupId, CompletableFuture<RaftServerImpl> future) {
      final RaftServerImpl impl;
      try {
        impl = future.join();
      } catch (Throwable t) {
        LOG.warn("{}: Failed to join the division for {}", getId(), groupId, t);
        return;
      }
      try {
        impl.close();
      } catch (Throwable t) {
        LOG.warn("{}: Failed to close the division for {}", getId(), groupId, t);
      }
      impl.getStateMachine().event().notifyServerShutdown(impl.getRoleInfoProto(), true);
    }

    synchronized List<RaftGroupId> getGroupIds() {
      return new ArrayList<>(map.keySet());
    }

    synchronized List<CompletableFuture<RaftServerImpl>> getAll() {
      return new ArrayList<>(map.values());
    }

    CompletableFuture<RaftServerImpl> get(RaftGroupId groupId) {
      final CompletableFuture<RaftServerImpl> i = map.get(groupId);
      if (i == null) {
        return JavaUtils.completeExceptionally(new GroupMismatchException(
            getId() + ": " + groupId + " not found."));
      }
      return i;
    }

    boolean containsGroup(RaftGroupId groupId) {
      return map.containsKey(groupId);
    }

    @Override
    public synchronized String toString() {
      if (map.isEmpty()) {
        return "<EMPTY>";
      } else if (map.size() == 1) {
        return toString(map.entrySet().iterator().next());
      }
      final StringBuilder b = new StringBuilder("[");
      map.entrySet().forEach(e -> b.append("\n  ").append(toString(e)));
      return b.append("] size=").append(map.size()).toString();
    }

    String toString(Map.Entry<RaftGroupId, CompletableFuture<RaftServerImpl>> e) {
      return toString(e.getKey(), e.getValue());
    }

    String toString(RaftGroupId groupId, CompletableFuture<RaftServerImpl> f) {
      return "" + (f != null && f.isDone()? f.join(): groupId + ":" + f);
    }
  }

  private final RaftPeerId id;
  private final Supplier<RaftPeer> peerSupplier = JavaUtils.memoize(this::buildRaftPeer);
  private final RaftProperties properties;
  private final StateMachine.Registry stateMachineRegistry;
  private final LifeCycle lifeCycle;

  private final RaftServerRpc serverRpc;
  private final ServerFactory factory;

  private final DataStreamServerRpc dataStreamServerRpc;

  private final ImplMap impls = new ImplMap();
  private final MemoizedSupplier<ExecutorService> implExecutor;
  private final MemoizedSupplier<ExecutorService> executor;

  private final JvmPauseMonitor pauseMonitor;
  private final ThreadGroup threadGroup;

  RaftServerProxy(RaftPeerId id, StateMachine.Registry stateMachineRegistry,
      RaftProperties properties, Parameters parameters, ThreadGroup threadGroup) {
    this.properties = properties;
    this.stateMachineRegistry = stateMachineRegistry;

    final RpcType rpcType = RaftConfigKeys.Rpc.type(properties, LOG::info);
    this.factory = ServerFactory.cast(rpcType.newFactory(parameters));

    this.serverRpc = factory.newRaftServerRpc(this);

    this.id = id != null? id: RaftPeerId.valueOf(getIdStringFrom(serverRpc));
    this.lifeCycle = new LifeCycle(this.id + "-" + JavaUtils.getClassSimpleName(getClass()));

    this.dataStreamServerRpc = new DataStreamServerImpl(this, parameters).getServerRpc();

    this.implExecutor = MemoizedSupplier.valueOf(
        () -> Concurrents3.newSingleThreadExecutor(id + "-groupManagement"));
    this.executor = MemoizedSupplier.valueOf(() -> Concurrents3.newThreadPoolWithMax(
        RaftServerConfigKeys.ThreadPool.proxyCached(properties),
        RaftServerConfigKeys.ThreadPool.proxySize(properties),
        id + "-impl"));

    final TimeDuration sleepDeviationThreshold = RaftServerConfigKeys.sleepDeviationThreshold(properties);
    final TimeDuration closeThreshold = RaftServerConfigKeys.closeThreshold(properties);
    final TimeDuration leaderStepDownWaitTime = RaftServerConfigKeys.LeaderElection.leaderStepDownWaitTime(properties);
    this.pauseMonitor = JvmPauseMonitor.newBuilder().setName(id)
        .setSleepDeviationThreshold(sleepDeviationThreshold)
        .setHandler(extraSleep -> handleJvmPause(extraSleep, closeThreshold, leaderStepDownWaitTime))
        .build();
    this.threadGroup = threadGroup == null ? new ThreadGroup(this.id.toString()) : threadGroup;
  }

  private void handleJvmPause(TimeDuration extraSleep, TimeDuration closeThreshold, TimeDuration stepDownThreshold)
      throws IOException {
    if (extraSleep.compareTo(closeThreshold) > 0) {
      LOG.error("{}: JVM pause detected {} longer than the close-threshold {}, shutting down ...",
          getId(), extraSleep.toString(TimeUnit.SECONDS, 3), closeThreshold.toString(TimeUnit.SECONDS, 3));
      close();
    } else if (extraSleep.compareTo(stepDownThreshold) > 0) {
      LOG.warn("{}: JVM pause detected {} longer than the step-down-threshold {}",
          getId(), extraSleep.toString(TimeUnit.SECONDS, 3), stepDownThreshold.toString(TimeUnit.SECONDS, 3));
      getImpls().forEach(RaftServerImpl::stepDownOnJvmPause);
    }
  }

  /** Check the storage dir and add groups*/
  void initGroups(RaftGroup group, StartupOption option) {
    final Optional<RaftGroup> raftGroup = Optional.ofNullable(group);
    //暂时不再加载其他group的数据
    /*final RaftGroupId raftGroupId = raftGroup.map(RaftGroup::getGroupId).orElse(null);
    final Predicate<RaftGroupId> shouldAdd = gid -> gid != null && !gid.equals(raftGroupId);
    Concurrents3.parallelForEachAsync(RaftServerConfigKeys.storageDir(properties),
        dir -> Optional.ofNullable(dir.listFiles())
            .map(Arrays::stream).orElse(Stream.empty())
            .filter(File::isDirectory)
            .forEach(sub -> initGroupDir(sub, shouldAdd)),
        executor.get()).join();*/
    raftGroup.ifPresent(g -> addGroup(g, option));
  }

  private void initGroupDir(File sub, Predicate<RaftGroupId> shouldAdd) {
    try {
      LOG.info("{}: found a subdirectory {}", getId(), sub);
      RaftGroupId groupId = null;
      try {
        groupId = RaftGroupId.valueOf(sub.getName());
      } catch (Exception e) {
        LOG.info("{}: The directory {} is not a group directory;" +
            " ignoring it. ", getId(), sub.getAbsolutePath());
      }
      if (shouldAdd.test(groupId)) {
        addGroup(RaftGroup.valueOf(groupId), StartupOption.RECOVER);
      }
    } catch (Exception e) {
      LOG.warn(getId() + ": Failed to initialize the group directory "
          + sub.getAbsolutePath() + ".  Ignoring it", e);
    }
  }

  void addRaftPeers(Collection<RaftPeer> peers) {
    final List<RaftPeer> others = peers.stream().filter(p -> !p.getId().equals(getId())).collect(Collectors.toList());
    getServerRpc().addRaftPeers(others);
    getDataStreamServerRpc().addRaftPeers(others);
  }

  private CompletableFuture<RaftServerImpl> newRaftServerImpl(RaftGroup group, StartupOption option) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        addRaftPeers(group.getPeers());
        return new RaftServerImpl(group, stateMachineRegistry.apply(group.getGroupId()), this, option);
      } catch(IOException e) {
        throw new CompletionException(getId() + ": Failed to initialize server for " + group, e);
      }
    }, implExecutor.get());
  }

  private static String getIdStringFrom(RaftServerRpc rpc) {
    InetSocketAddress address = null;
    try {
      address = rpc.getInetSocketAddress();
    } catch(Exception e) {
      LOG.warn("Failed to get InetSocketAddress from " + rpc.getRpcType() + " rpc server", e);
    }
    return address != null? address.getHostName() + "_" + address.getPort()
        : rpc.getRpcType() + "-" + UUID.randomUUID();
  }

  @Override
  public RaftPeerId getId() {
    return id;
  }

  @Override
  public RaftPeer getPeer() {
    return peerSupplier.get();
  }

  private RaftPeer buildRaftPeer() {
    return RaftPeer.newBuilder()
        .setId(getId())
        .setAddress(getServerRpc().getInetSocketAddress())
        .setDataStreamAddress(getDataStreamServerRpc().getInetSocketAddress())
        .setClientAddress(getServerRpc().getClientServerAddress())
        .setAdminAddress(getServerRpc().getAdminServerAddress())
        .build();
  }

  @Override
  public List<RaftGroupId> getGroupIds() {
    return impls.getGroupIds();
  }

  @Override
  public Iterable<RaftGroup> getGroups() throws IOException {
    return getImpls().stream().map(RaftServerImpl::getGroup).collect(Collectors.toList());
  }

  @Override
  public ServerFactory getFactory() {
    return factory;
  }

  @Override
  public RaftProperties getProperties() {
    return properties;
  }

  @Override
  public RaftServerRpc getServerRpc() {
    return serverRpc;
  }

  @Override
  public DataStreamServerRpc getDataStreamServerRpc() {
    return dataStreamServerRpc;
  }

  private CompletableFuture<RaftServerImpl> addGroup(RaftGroup group, StartupOption option) {
    return impls.addNew(group, option);
  }

  private CompletableFuture<RaftServerImpl> getImplFuture(RaftGroupId groupId) {
    return impls.get(groupId);
  }

  private RaftServerImpl getImpl(RaftRpcRequestProto proto) throws IOException {
    return getImpl(ProtoUtils.toRaftGroupId(proto.getRaftGroupId()));
  }

  private RaftServerImpl getImpl(RaftGroupId groupId) throws IOException {
    Objects.requireNonNull(groupId, "groupId == null");
    return IOUtils.getFromFuture(getImplFuture(groupId), this::getId);
  }

  List<RaftServerImpl> getImpls() throws IOException {
    final List<RaftServerImpl> list = new ArrayList<>();
    for(CompletableFuture<RaftServerImpl> f : impls.getAll()) {
      list.add(IOUtils.getFromFuture(f, this::getId));
    }
    return list;
  }

  @Override
  public Division getDivision(RaftGroupId groupId) throws IOException {
    return getImpl(groupId);
  }

  @Override
  public LifeCycle.State getLifeCycleState() {
    return lifeCycle.getCurrentState();
  }

  ThreadGroup getThreadGroup() {
    return threadGroup;
  }

  @Override
  public void start() throws IOException {
    lifeCycle.startAndTransition(this::startImpl, IOException.class);
  }

  private void startImpl() throws IOException {
    Concurrents3.parallelForEachAsync(getImpls(), RaftServerImpl::start, executor.get()).join();

    LOG.info("{}: start RPC server", getId());
    getServerRpc().start();
    getDataStreamServerRpc().start();

    pauseMonitor.start();
  }

  @Override
  public void close() {
    lifeCycle.checkStateAndClose(() -> {
      LOG.info("{}: close", getId());
      impls.close();

      try {
        getServerRpc().close();
      } catch(IOException ignored) {
        LOG.warn(getId() + ": Failed to close " + getRpcType() + " server", ignored);
      }

      try {
        getDataStreamServerRpc().close();
      } catch (IOException ignored) {
        LOG.warn(getId() + ": Failed to close " + SupportedDataStreamType.NETTY + " server", ignored);
      }

      try {
        Concurrents3.shutdownAndWait(implExecutor.get());
      } catch (Exception ignored) {
        LOG.warn(getId() + ": Failed to shutdown implExecutor", ignored);
      }

      try {
        Concurrents3.shutdownAndWait(executor.get());
      } catch (Exception ignored) {
        LOG.warn(getId() + ": Failed to shutdown executor", ignored);
      }
    });
    pauseMonitor.stop();
  }

  @Override
  public CompletableFuture<RaftClientReply> submitClientRequestAsync(
      ReferenceCountedObject<RaftClientRequest> requestRef) {
    final RaftClientRequest request = requestRef.retain();
    try {
      return getImplFuture(request.getRaftGroupId())
          .thenCompose(impl -> impl.executeSubmitClientRequestAsync(requestRef));
    } finally {
      requestRef.release();
    }
  }

  @Override
  public RaftClientReply submitClientRequest(RaftClientRequest request)
      throws IOException {
    return getImpl(request.getRaftGroupId()).submitClientRequest(request);
  }

  @Override
  public RaftClientReply setConfiguration(SetConfigurationRequest request)
      throws IOException {
    return getImpl(request.getRaftGroupId()).setConfiguration(request);
  }

  @Override
  public RaftClientReply transferLeadership(TransferLeadershipRequest request)
      throws IOException {
    return getImpl(request.getRaftGroupId()).transferLeadership(request);
  }

  @Override
  public <T, R> DRpcReply<R> invokeRpc(DRpcRequest<T, R> request) throws IOException {
    return RaftServerImpl.waitForReply(getId(), request, invokeRpcAsync(request), r -> null);
  }

  @Override
  public RaftClientReply groupManagement(GroupManagementRequest request) throws IOException {
    return RaftServerImpl.waitForReply(getId(), request, groupManagementAsync(request),
        e -> RaftClientReply.newBuilder()
            .setRequest(request)
            .setException(e)
            .build());
  }

  @Override
  public CompletableFuture<RaftClientReply> groupManagementAsync(GroupManagementRequest request) {
    final RaftGroupId groupId = request.getRaftGroupId();
    if (groupId == null) {
      return JavaUtils.completeExceptionally(new GroupMismatchException(
          getId() + ": Request group id == null"));
    }
    final GroupManagementRequest.Add add = request.getAdd();
    if (add != null) {
      return groupAddAsync(request, add.getGroup(), add.isFormat());
    }
    final GroupManagementRequest.Remove remove = request.getRemove();
    if (remove != null) {
      return groupRemoveAsync(request, remove.getGroupId(),
          remove.isDeleteDirectory(), remove.isRenameDirectory());
    }
    return JavaUtils.completeExceptionally(new UnsupportedOperationException(
        getId() + ": Request not supported " + request));
  }

  private CompletableFuture<RaftClientReply> groupAddAsync(
      GroupManagementRequest request, RaftGroup newGroup, boolean format) {
    if (!request.getRaftGroupId().equals(newGroup.getGroupId())) {
      return JavaUtils.completeExceptionally(new GroupMismatchException(
          getId() + ": Request group id (" + request.getRaftGroupId() + ") does not match the new group " + newGroup));
    }
    return impls.addNew(newGroup, format? StartupOption.FORMAT: StartupOption.RECOVER)
        .thenApplyAsync(newImpl -> {
          LOG.debug("{}: newImpl = {}", getId(), newImpl);
          try {
            final boolean started = newImpl.start();
            Preconditions.assertTrue(started, () -> getId()+ ": failed to start a new impl: " + newImpl);
          } catch (IOException e) {
            throw new CompletionException(e);
          }
          return newImpl.newSuccessReply(request);
        }, implExecutor.get())
        .whenComplete((raftClientReply, throwable) -> {
          if (throwable != null) {
            if (!(throwable.getCause() instanceof AlreadyExistsException)) {
              impls.remove(newGroup.getGroupId());
              LOG.warn(getId() + ": Failed groupAdd* " + request, throwable);
            } else {
              if (LOG.isDebugEnabled()) {
                LOG.debug(getId() + ": Failed groupAdd* " + request, throwable);
              }
            }
          }
        });
  }

  private CompletableFuture<RaftClientReply> groupRemoveAsync(
      RaftClientRequest request, RaftGroupId groupId, boolean deleteDirectory,
      boolean renameDirectory) {
    if (!request.getRaftGroupId().equals(groupId)) {
      return JavaUtils.completeExceptionally(new GroupMismatchException(
          getId() + ": Request group id (" + request.getRaftGroupId() + ") does not match the given group id " +
              groupId));
    }
    final CompletableFuture<RaftServerImpl> f = impls.remove(groupId);
    if (f == null) {
      return JavaUtils.completeExceptionally(new GroupMismatchException(
          getId() + ": Group " + groupId + " not found."));
    }
    return f.thenApply(impl -> {
      impl.groupRemove(deleteDirectory, renameDirectory);
      return impl.newSuccessReply(request);
    });
  }

  @Override
  public RaftClientReply snapshotManagement(SnapshotManagementRequest request) throws IOException {
    return RaftServerImpl.waitForReply(getId(), request, snapshotManagementAsync(request),
          e -> RaftClientReply.newBuilder()
                .setRequest(request)
                .setException(e)
                .build());
  }

  @Override
  public CompletableFuture<RaftClientReply> snapshotManagementAsync(SnapshotManagementRequest request) {
    final RaftGroupId groupId = request.getRaftGroupId();
    if (groupId == null) {
      return JavaUtils.completeExceptionally(new GroupMismatchException(
            getId() + ": Request group id == null"));
    }
    final SnapshotManagementRequest.Create create = request.getCreate();
    if (create != null) {
      return createAsync(request);
    }
    return JavaUtils.completeExceptionally(new UnsupportedOperationException(
          getId() + ": Request not supported " + request));
  }

  private CompletableFuture<RaftClientReply> createAsync(SnapshotManagementRequest request) {
    return getImplFuture(request.getRaftGroupId())
        .thenCompose(impl -> impl.executeSubmitServerRequestAsync(() -> impl.takeSnapshotAsync(request)));
  }

  @Override
  public RaftClientReply leaderElectionManagement(LeaderElectionManagementRequest request) throws IOException {
    return RaftServerImpl.waitForReply(getId(), request, leaderElectionManagementAsync(request),
        e -> RaftClientReply.newBuilder()
            .setRequest(request)
            .setException(e)
            .build());
  }

  @Override
  public CompletableFuture<RaftClientReply> leaderElectionManagementAsync(
      LeaderElectionManagementRequest request) {
    return getImplFuture(request.getRaftGroupId())
        .thenCompose(impl -> impl.executeSubmitServerRequestAsync(() -> impl.leaderElectionManagementAsync(request)));
  }

  @Override
  public GroupListReply getGroupList(GroupListRequest request) {
    return new GroupListReply(request, getGroupIds());
  }

  @Override
  public CompletableFuture<GroupListReply> getGroupListAsync(GroupListRequest request) {
    return CompletableFuture.completedFuture(getGroupList(request));
  }

  @Override
  public GroupInfoReply getGroupInfo(GroupInfoRequest request) throws IOException {
    return RaftServerImpl.waitForReply(getId(), request, getGroupInfoAsync(request), r -> null);
  }

  @Override
  public CompletableFuture<GroupInfoReply> getGroupInfoAsync(GroupInfoRequest request) {
    return getImplFuture(request.getRaftGroupId()).thenApplyAsync(
        server -> server.getGroupInfo(request));
  }

  /**
   * Handle a raft configuration change request from client.
   */
  @Override
  public CompletableFuture<RaftClientReply> setConfigurationAsync(SetConfigurationRequest request) {
    return getImplFuture(request.getRaftGroupId())
        .thenCompose(impl -> impl.executeSubmitServerRequestAsync(() -> impl.setConfigurationAsync(request)));
  }

  @Override
  public CompletableFuture<RaftClientReply> transferLeadershipAsync(TransferLeadershipRequest request) {
    return getImplFuture(request.getRaftGroupId())
        .thenCompose(impl -> impl.executeSubmitServerRequestAsync(() -> impl.transferLeadershipAsync(request)));
  }

  @Override
  public <T, R> CompletableFuture<DRpcReply<R>> invokeRpcAsync(DRpcRequest<T, R> request) {
    return CompletableFuture.supplyAsync(()->{
      Exception ex= null;
      R data = null;
      try {
        if(request.getTarget()!=null) {
          T bean = getBean(request.getTarget()).orElse(null);
          if (bean != null) {
            data = request.getFun().apply(bean);
          } else {
            ex = new BeanNotFindException(request.getTarget());
          }
        }else{
          data = request.getFun().apply(null);
        }

      } catch (Exception e) {
        ex = e;
      }

      return new DRpcReply<R>(request, data, ex);
    });
  }

  @Override
  public RequestVoteReplyProto requestVote(RequestVoteRequestProto request) throws IOException {
    return getImpl(request.getServerRequest()).requestVote(request);
  }

  @Override
  public StartLeaderElectionReplyProto startLeaderElection(StartLeaderElectionRequestProto request) throws IOException {
    return getImpl(request.getServerRequest()).startLeaderElection(request);
  }

  @Override
  public CompletableFuture<AppendEntriesReplyProto> appendEntriesAsync(
      ReferenceCountedObject<AppendEntriesRequestProto> requestRef) {
    AppendEntriesRequestProto request = requestRef.retain();
    try {
      final RaftGroupId groupId = ProtoUtils.toRaftGroupId(request.getServerRequest().getRaftGroupId());
      return getImplFuture(groupId)
          .thenCompose(impl -> impl.executeSubmitServerRequestAsync(() -> impl.appendEntriesAsync(requestRef)));
    } finally {
      requestRef.release();
    }
  }

  @Override
  public CompletableFuture<ReadIndexReplyProto> readIndexAsync(ReadIndexRequestProto request) throws IOException {
    final RaftGroupId groupId = ProtoUtils.toRaftGroupId(request.getServerRequest().getRaftGroupId());
    return getImplFuture(groupId)
        .thenCompose(impl -> impl.executeSubmitServerRequestAsync(() -> impl.readIndexAsync(request)));
  }

  @Override
  public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto request) throws IOException {
    return getImpl(request.getServerRequest()).appendEntries(request);
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(InstallSnapshotRequestProto request) throws IOException {
    return getImpl(request.getServerRequest()).installSnapshot(request);
  }

  @Override
  public String toString() {
    return getId() + String.format(":%9s ", lifeCycle.getCurrentState()) + impls;
  }
}
