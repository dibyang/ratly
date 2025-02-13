
package net.xdob.ratly.client.impl;

import net.xdob.ratly.client.DataStreamClient;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.client.RaftClientRpc;
import net.xdob.ratly.client.api.DRpcApi;
import net.xdob.ratly.client.api.DataStreamApi;
import net.xdob.ratly.client.api.LeaderElectionManagementApi;
import net.xdob.ratly.client.api.SnapshotManagementApi;
import net.xdob.ratly.client.retry.ClientRetryEvent;
import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.proto.raft.SlidingWindowEntry;
import net.xdob.ratly.protocol.ClientId;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftClientRequest;
import net.xdob.ratly.protocol.RaftGroup;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.protocol.exceptions.LeaderNotReadyException;
import net.xdob.ratly.protocol.exceptions.NotLeaderException;
import net.xdob.ratly.protocol.exceptions.RaftException;
import net.xdob.ratly.protocol.exceptions.RaftRetryFailureException;
import net.xdob.ratly.protocol.exceptions.ResourceUnavailableException;
import net.xdob.ratly.retry.RetryPolicy;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import net.xdob.ratly.util.Collections3;
import net.xdob.ratly.util.IOUtils;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.MemoizedSupplier;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.TimeDuration;
import net.xdob.ratly.util.TimeoutExecutor;
import net.xdob.ratly.util.Timestamp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/** A client who sends requests to a raft service. */
public final class RaftClientImpl implements RaftClient {
  private static final Cache<RaftGroupId, RaftPeerId> LEADER_CACHE = CacheBuilder.newBuilder()
      .expireAfterAccess(60, TimeUnit.SECONDS)
      .maximumSize(1024)
      .build();

  public abstract static class PendingClientRequest {
    private final Timestamp creationTime = Timestamp.currentTime();
    private final CompletableFuture<RaftClientReply> replyFuture = new CompletableFuture<>();
    private final AtomicInteger attemptCount = new AtomicInteger();
    private final Map<Class<?>, Integer> exceptionCounts = new ConcurrentHashMap<>();

    public abstract RaftClientRequest newRequestImpl();

    final RaftClientRequest newRequest() {
      final int attempt = attemptCount.incrementAndGet();
      final RaftClientRequest request = newRequestImpl();
      LOG.debug("attempt #{}, newRequest {}", attempt, request);
      return request;
    }

    CompletableFuture<RaftClientReply> getReplyFuture() {
      return replyFuture;
    }

    public int getAttemptCount() {
      return attemptCount.get();
    }

    public ClientRetryEvent newClientRetryEvent(RaftClientRequest request, Throwable throwable) {
      final int exceptionCount = throwable == null? 0
          : exceptionCounts.compute(throwable.getClass(), (k, v) -> v == null? 1: v+1);
      return new ClientRetryEvent(getAttemptCount(), request, exceptionCount, throwable, creationTime);
    }
  }

  static class RaftPeerList implements Iterable<RaftPeer> {
    private final AtomicReference<List<RaftPeer>> list = new AtomicReference<>();

    @Override
    public Iterator<RaftPeer> iterator() {
      return list.get().iterator();
    }

    void set(Collection<RaftPeer> newPeers) {
      Preconditions.assertTrue(!newPeers.isEmpty(), "newPeers is empty.");
      list.set(Collections.unmodifiableList(new ArrayList<>(newPeers)));
    }
  }

  static class RepliedCallIds {
    private final Object name;

    // 使用 AtomicReference 管理 replied 集合，并采用线程安全且有序的 ConcurrentSkipListSet
    private final AtomicReference<Set<Long>> repliedRef =
        new AtomicReference<>(new ConcurrentSkipListSet<>());

    // 使用 Guava Cache 限制 sent 的最大容量，并设置自动过期时间（例如：2 分钟内未访问则过期）
    private final Cache<Long, Set<Long>> sent = CacheBuilder.newBuilder()
        .maximumSize(100000) // 根据实际情况调整容量
        .expireAfterAccess(2, TimeUnit.MINUTES)
        .build();



    RepliedCallIds(Object name) {
      this.name = name;
    }

    /** 标记 repliedCallId 已经回复，同时从 sent 中清除对应记录 */
    void add(long repliedCallId) {
      LOG.debug("{}: add replied callId {}", name, repliedCallId);
      // 无锁添加到 replied 集合中
      repliedRef.get().add(repliedCallId);
      // 从缓存中失效对应的 entry
      sent.invalidate(repliedCallId);
    }

    /**
     * 获取对应 callId 的 replied callIds 集合。
     * 当 cache 中不存在该 callId 时，调用 getAndReset() 获取当前 replied 集合的快照，并自动重置。
     */
    Iterable<Long> get(long callId) {
      try {
        Set<Long> set = sent.get(callId, () -> {
          // 当 callId 首次出现时，返回 replied 集合的快照（并重置 replied）
          return Collections.unmodifiableSet(getAndReset());
        });
        LOG.debug("{}: get {} returns {}", name, callId, set);
        return set;
      } catch (ExecutionException e) {
        throw new RuntimeException("Error retrieving replied callIds for callId: " + callId, e);
      }
    }

    private synchronized Set<Long> getAndReset() {
      return  repliedRef.getAndSet(new ConcurrentSkipListSet<>());
    }
  }

  private final ClientId clientId;
  private final RaftClientRpc clientRpc;
  private final RaftPeerList peers = new RaftPeerList();
  private final RaftGroupId groupId;
  private final RetryPolicy retryPolicy;

  @SuppressWarnings({"squid:S3077"}) // Suppress volatile for generic type
  private volatile RaftPeerId leaderId;
  /** The callIds of the replied requests. */
  private final RepliedCallIds repliedCallIds;

  private final TimeoutExecutor scheduler = TimeoutExecutor.getInstance();

  private final Supplier<OrderedAsync> orderedAsync;
  private final Supplier<AsyncImpl> asyncApi;
  private final Supplier<BlockingImpl> blockingApi;
  private final Supplier<MessageStreamImpl> messageStreamApi;
  private final MemoizedSupplier<DataStreamApi> dataStreamApi;

  private final Supplier<AdminImpl> adminApi;
  private final ConcurrentMap<RaftPeerId, GroupManagementImpl> groupManagement = new ConcurrentHashMap<>();
  private final ConcurrentMap<RaftPeerId, SnapshotManagementApi> snapshotManagement = new ConcurrentHashMap<>();
  private final ConcurrentMap<RaftPeerId, LeaderElectionManagementApi>
      leaderElectionManagement = new ConcurrentHashMap<>();
  private final ConcurrentMap<RaftPeerId, DRpcApiImpl> dRpcApi = new ConcurrentHashMap<>();

  private final AtomicBoolean closed = new AtomicBoolean();

  @SuppressWarnings("checkstyle:ParameterNumber")
  RaftClientImpl(ClientId clientId, RaftGroup group, RaftPeerId leaderId, RaftPeer primaryDataStreamServer,
      RaftClientRpc clientRpc, RetryPolicy retryPolicy, RaftProperties properties, Parameters parameters) {
    this.clientId = clientId;
    this.peers.set(group.getPeers());
    this.groupId = group.getGroupId();

    this.leaderId = Objects.requireNonNull(computeLeaderId(leaderId, group),
        () -> "this.leaderId is set to null, leaderId=" + leaderId + ", group=" + group);
    this.repliedCallIds = new RepliedCallIds(clientId);
    this.retryPolicy = Objects.requireNonNull(retryPolicy, "retry policy can't be null");

    clientRpc.addRaftPeers(group.getPeers());
    this.clientRpc = clientRpc;

    this.orderedAsync = JavaUtils.memoize(() -> OrderedAsync.newInstance(this, properties));
    this.messageStreamApi = JavaUtils.memoize(() -> MessageStreamImpl.newInstance(this, properties));
    this.asyncApi = JavaUtils.memoize(() -> new AsyncImpl(this));
    this.blockingApi = JavaUtils.memoize(() -> new BlockingImpl(this));
    this.dataStreamApi = JavaUtils.memoize(() -> DataStreamClient.newBuilder(this)
        .setDataStreamServer(primaryDataStreamServer)
        .setProperties(properties)
        .setParameters(parameters)
        .build());
    this.adminApi = JavaUtils.memoize(() -> new AdminImpl(this));
  }

  @Override
  public RaftPeerId getLeaderId() {
    return leaderId;
  }

  @Override
  public RaftGroupId getGroupId() {
    return groupId;
  }

  private static RaftPeerId computeLeaderId(RaftPeerId leaderId, RaftGroup group) {
    if (leaderId != null) {
      return leaderId;
    }
    final RaftPeerId cached = LEADER_CACHE.getIfPresent(group.getGroupId());
    if (cached != null && group.getPeer(cached) != null) {
      return cached;
    }
    return getHighestPriorityPeer(group).getId();
  }

  private static RaftPeer getHighestPriorityPeer(RaftGroup group) {
    final Iterator<RaftPeer> i = group.getPeers().iterator();
    if (!i.hasNext()) {
      throw new IllegalArgumentException("Group peers is empty in " + group);
    }

    RaftPeer highest = i.next();
    for(; i.hasNext(); ) {
      final RaftPeer peer = i.next();
      if (peer.getPriority() > highest.getPriority()) {
        highest = peer;
      }
    }
    return highest;
  }

  @Override
  public ClientId getId() {
    return clientId;
  }

  RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  TimeDuration getEffectiveSleepTime(Throwable t, TimeDuration sleepDefault) {
    return t instanceof NotLeaderException && ((NotLeaderException) t).getSuggestedLeader() != null ?
        TimeDuration.ZERO : sleepDefault;
  }

  TimeoutExecutor getScheduler() {
    return scheduler;
  }

  OrderedAsync getOrderedAsync() {
    return orderedAsync.get();
  }

  RaftClientRequest newRaftClientRequest(
      RaftPeerId server, long callId, Message message, RaftClientRequest.Type type,
      SlidingWindowEntry slidingWindowEntry) {
    final RaftClientRequest.Builder b = RaftClientRequest.newBuilder();
    if (server != null) {
      b.setServerId(server);
    } else {
      b.setLeaderId(getLeaderId())
       .setRepliedCallIds(repliedCallIds.get(callId));
    }
    return b.setClientId(clientId)
        .setGroupId(groupId)
        .setCallId(callId)
        .setMessage(message)
        .setType(type)
        .setSlidingWindowEntry(slidingWindowEntry)
        .build();
  }

  @Override
  public AdminImpl admin() {
    return adminApi.get();
  }

  @Override
  public GroupManagementImpl getGroupManagementApi(RaftPeerId server) {
    return groupManagement.computeIfAbsent(server, id -> new GroupManagementImpl(id, this));
  }

  @Override
  public SnapshotManagementApi getSnapshotManagementApi() {
    return JavaUtils.memoize(() -> new SnapshotManagementImpl(null, this)).get();
  }

  @Override
  public SnapshotManagementApi getSnapshotManagementApi(RaftPeerId server) {
    return snapshotManagement.computeIfAbsent(server, id -> new SnapshotManagementImpl(id, this));
  }

  @Override
  public LeaderElectionManagementApi getLeaderElectionManagementApi(RaftPeerId server) {
    return leaderElectionManagement.computeIfAbsent(server, id -> new LeaderElectionManagementImpl(id, this));
  }

  @Override
  public DRpcApi getDRpcApiApi(RaftPeerId server) {
    return dRpcApi.computeIfAbsent(server, id -> new DRpcApiImpl(id, this));
  }

  @Override
  public BlockingImpl io() {
    return blockingApi.get();
  }

  @Override
  public AsyncImpl async() {
    return asyncApi.get();
  }

  @Override
  public MessageStreamImpl getMessageStreamApi() {
    return messageStreamApi.get();
  }

  @Override
  public DataStreamApi getDataStreamApi() {
    return dataStreamApi.get();
  }

  IOException noMoreRetries(ClientRetryEvent event) {
    final int attemptCount = event.getAttemptCount();
    final Throwable throwable = event.getCause();
    if (attemptCount == 1 && throwable != null) {
      return IOUtils.asIOException(throwable);
    }
    return new RaftRetryFailureException(event.getRequest(), attemptCount, retryPolicy, throwable);
  }

  RaftClientReply handleReply(RaftClientRequest request, RaftClientReply reply) {
    if (request.isToLeader() && reply != null) {
      if (!request.getType().isReadOnly()) {
        repliedCallIds.add(reply.getCallId());
      }

      if (reply.getException() == null) {
        LEADER_CACHE.put(reply.getRaftGroupId(), reply.getServerId());
      }
    }
    return reply;
  }

  static <E extends Throwable> RaftClientReply handleRaftException(
      RaftClientReply reply, Function<RaftException, E> converter) throws E {
    if (reply != null) {
      final RaftException e = reply.getException();
      if (e != null) {
        throw converter.apply(e);
      }
    }
    return reply;
  }

  /**
   * @return null if the reply is null or it has
   * {@link NotLeaderException} or {@link LeaderNotReadyException}
   * otherwise return the same reply.
   */
  RaftClientReply handleLeaderException(RaftClientRequest request, RaftClientReply reply) {
    if (reply == null || reply.getException() instanceof LeaderNotReadyException) {
      return null;
    }
    final NotLeaderException nle = reply.getNotLeaderException();
    if (nle == null) {
      return reply;
    }
    return handleNotLeaderException(request, nle, null);
  }

  RaftClientReply handleNotLeaderException(RaftClientRequest request, NotLeaderException nle,
      Consumer<RaftClientRequest> handler) {
    refreshPeers(nle.getPeers());
    final RaftPeerId newLeader = nle.getSuggestedLeader() == null ? null
        : nle.getSuggestedLeader().getId();
    handleIOException(request, nle, newLeader, handler);
    return null;
  }

  private void refreshPeers(Collection<RaftPeer> newPeers) {
    if (newPeers != null && !newPeers.isEmpty()) {
      peers.set(newPeers);
      // also refresh the rpc proxies for these peers
      clientRpc.addRaftPeers(newPeers);
    }
  }

  void handleIOException(RaftClientRequest request, IOException ioe) {
    handleIOException(request, ioe, null, null);
  }

  void handleIOException(RaftClientRequest request, IOException ioe,
      RaftPeerId newLeader, Consumer<RaftClientRequest> handler) {
    LOG.debug("{}: suggested new leader: {}. Failed {}", clientId, newLeader, request, ioe);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Stack trace", new Throwable("TRACE"));
    }

    Optional.ofNullable(handler).ifPresent(h -> h.accept(request));

    if (ioe instanceof LeaderNotReadyException || ioe instanceof ResourceUnavailableException) {
      return;
    }

    final RaftPeerId oldLeader = request.getServerId();
    final RaftPeerId curLeader = getLeaderId();
    final boolean stillLeader = oldLeader.equals(curLeader);
    if (newLeader == null && stillLeader) {
      newLeader = Collections3.random(oldLeader,
          Collections3.as(peers, RaftPeer::getId));
    }
    LOG.debug("{}: oldLeader={},  curLeader={}, newLeader={}", clientId, oldLeader, curLeader, newLeader);

    final boolean changeLeader = newLeader != null && stillLeader;
    final boolean reconnect = changeLeader || clientRpc.shouldReconnect(ioe);
    if (reconnect) {
      if (changeLeader && oldLeader.equals(getLeaderId())) {
        LOG.debug("{} changes Leader from {} to {} for {}",
            clientId, oldLeader, newLeader, groupId, ioe);
        this.leaderId = newLeader;
      }
      clientRpc.handleException(oldLeader, ioe, true);
    }
  }

  @Override
  public RaftClientRpc getClientRpc() {
    return clientRpc;
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public void close() throws IOException {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    LOG.debug("close {}", getId());
    clientRpc.close();
    if (dataStreamApi.isInitialized()) {
      dataStreamApi.get().close();
    }
  }
}
