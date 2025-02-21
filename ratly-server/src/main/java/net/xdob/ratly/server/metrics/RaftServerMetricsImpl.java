package net.xdob.ratly.server.metrics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import net.xdob.ratly.metrics.LongCounter;
import net.xdob.ratly.metrics.Timekeeper;

import net.xdob.ratly.metrics.MetricRegistryInfo;
import net.xdob.ratly.metrics.RatlyMetricRegistry;
import net.xdob.ratly.proto.raft.ReplicationLevel;
import net.xdob.ratly.proto.raft.RaftClientRequestProto.TypeCase;
import net.xdob.ratly.protocol.RaftClientRequest.Type;
import net.xdob.ratly.protocol.RaftGroupMemberId;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.metrics.RatlyMetrics;
import net.xdob.ratly.server.RetryCache;
import com.google.common.annotations.VisibleForTesting;

/**
 * Metric Registry for Raft Group Server. One instance per leader/follower.
 */
public final class RaftServerMetricsImpl extends RatlyMetrics implements RaftServerMetrics {

  public static final String RATLY_SERVER_METRICS = "server";
  public static final String RATLY_SERVER_METRICS_DESC = "Metrics for Raft server";

  public static final String FOLLOWER_LAST_HEARTBEAT_ELAPSED_TIME_METRIC = "%s_lastHeartbeatElapsedTime";
  public static final String FOLLOWER_APPEND_ENTRIES_LATENCY = "follower_append_entry_latency";
  public static final String LEADER_METRIC_PEER_COMMIT_INDEX = "%s_peerCommitIndex";
  public static final String RAFT_CLIENT_READ_REQUEST = "clientReadRequest";
  public static final String RAFT_CLIENT_STALE_READ_REQUEST = "clientStaleReadRequest";
  public static final String RAFT_CLIENT_WRITE_REQUEST = "clientWriteRequest";
  public static final String RAFT_CLIENT_WATCH_REQUEST = "clientWatch%sRequest";

  public static final String REQUEST_QUEUE_LIMIT_HIT_COUNTER = "numRequestQueueLimitHits";
  public static final String REQUEST_BYTE_SIZE_LIMIT_HIT_COUNTER = "numRequestsByteSizeLimitHits";
  public static final String RESOURCE_LIMIT_HIT_COUNTER = "numResourceLimitHits";
  public static final String WATCH_REQUEST_QUEUE_LIMIT_HIT_COUNTER = "numWatch%sRequestQueueLimitHits";

  public static final String REQUEST_QUEUE_SIZE = "numPendingRequestInQueue";
  public static final String REQUEST_MEGA_BYTE_SIZE = "numPendingRequestMegaByteSize";

  public static final String WATCH_REQUEST_QUEUE_SIZE = "numWatch%sRequestInQueue";
  public static final String WATCH_REQUEST_TIMEOUT_COUNTER = "numWatch%sRequestTimeout";

  public static final String RETRY_CACHE_ENTRY_COUNT_METRIC = "retryCacheEntryCount";
  public static final String RETRY_CACHE_HIT_COUNT_METRIC = "retryCacheHitCount";
  public static final String RETRY_CACHE_HIT_RATE_METRIC = "retryCacheHitRate";
  public static final String RETRY_CACHE_MISS_COUNT_METRIC = "retryCacheMissCount";
  public static final String RETRY_CACHE_MISS_RATE_METRIC = "retryCacheMissRate";

  public static final String RATLY_SERVER_FAILED_CLIENT_STALE_READ_COUNT = "numFailedClientStaleReadOnServer";
  public static final String RATLY_SERVER_FAILED_CLIENT_READ_COUNT       = "numFailedClientReadOnServer";
  public static final String RATLY_SERVER_FAILED_CLIENT_WRITE_COUNT      = "numFailedClientWriteOnServer";
  public static final String RATLY_SERVER_FAILED_CLIENT_WATCH_COUNT      = "numFailedClientWatchOnServer";
  public static final String RATLY_SERVER_FAILED_CLIENT_STREAM_COUNT     = "numFailedClientStreamOnServer";
  public static final String RATLY_SERVER_INSTALL_SNAPSHOT_COUNT = "numInstallSnapshot";

  private final LongCounter numRequestQueueLimitHits = getRegistry().counter(REQUEST_QUEUE_LIMIT_HIT_COUNTER);
  private final LongCounter numRequestsByteSizeLimitHits = getRegistry().counter(REQUEST_BYTE_SIZE_LIMIT_HIT_COUNTER);
  private final LongCounter numResourceLimitHits = getRegistry().counter(RESOURCE_LIMIT_HIT_COUNTER);
  private final Map<ReplicationLevel, LongCounter> numWatchRequestQueueLimitHits = newCounterMap(ReplicationLevel.class,
      replication -> getRegistry().counter(
          String.format(WATCH_REQUEST_QUEUE_LIMIT_HIT_COUNTER, Type.toString(replication))));
  private final Map<ReplicationLevel, LongCounter> numWatchRequestsTimeout = newCounterMap(ReplicationLevel.class,
      replication -> getRegistry().counter(String.format(WATCH_REQUEST_TIMEOUT_COUNTER, Type.toString(replication))));

  private final LongCounter numFailedClientStaleRead
      = getRegistry().counter(RATLY_SERVER_FAILED_CLIENT_STALE_READ_COUNT);
  private final LongCounter numFailedClientRead = getRegistry().counter(RATLY_SERVER_FAILED_CLIENT_READ_COUNT);
  private final LongCounter numFailedClientWrite = getRegistry().counter(RATLY_SERVER_FAILED_CLIENT_WRITE_COUNT);
  private final LongCounter numFailedClientWatch = getRegistry().counter(RATLY_SERVER_FAILED_CLIENT_WATCH_COUNT);
  private final LongCounter numFailedClientStream = getRegistry().counter(RATLY_SERVER_FAILED_CLIENT_STREAM_COUNT);

  private final LongCounter numInstallSnapshot = getRegistry().counter(RATLY_SERVER_INSTALL_SNAPSHOT_COUNT);

  private final Timekeeper readTimer = getRegistry().timer(RAFT_CLIENT_READ_REQUEST);
  private final Timekeeper staleReadTimer = getRegistry().timer(RAFT_CLIENT_STALE_READ_REQUEST);
  private final Timekeeper writeTimer = getRegistry().timer(RAFT_CLIENT_WRITE_REQUEST);
  private final Map<ReplicationLevel, Timekeeper> watchTimers = newTimerMap(ReplicationLevel.class,
      replication -> getRegistry().timer(String.format(RAFT_CLIENT_WATCH_REQUEST, Type.toString(replication))));

  private final Function<Boolean, Timekeeper> followerAppendEntryLatency
      = newHeartbeatTimer(FOLLOWER_APPEND_ENTRIES_LATENCY);

  /** Follower Id -> heartbeat elapsed */
  private final Map<RaftPeerId, Long> followerLastHeartbeatElapsedTimeMap = new ConcurrentHashMap<>();
  private final ToLongFunction<RaftPeerId> commitInfoCache;

  /** id -> metric */
  private static final Map<RaftGroupMemberId, RaftServerMetricsImpl> METRICS = new ConcurrentHashMap<>();
  /** id -> key */
  private static final Map<RaftPeerId, String> PEER_COMMIT_INDEX_GAUGE_KEYS = new ConcurrentHashMap<>();

  static String getPeerCommitIndexGaugeKey(RaftPeerId serverId) {
    return PEER_COMMIT_INDEX_GAUGE_KEYS.computeIfAbsent(serverId,
        key -> String.format(LEADER_METRIC_PEER_COMMIT_INDEX, key));
  }

  public static RaftServerMetricsImpl computeIfAbsentRaftServerMetrics(RaftGroupMemberId serverId,
      ToLongFunction<RaftPeerId> commitInfoCache,
      Supplier<RetryCache.Statistics> retryCacheStatistics) {
    return METRICS.computeIfAbsent(serverId,
        key -> new RaftServerMetricsImpl(serverId, commitInfoCache, retryCacheStatistics));
  }

  public static void removeRaftServerMetrics(RaftGroupMemberId serverId) {
    METRICS.remove(serverId);
  }

  public RaftServerMetricsImpl(RaftGroupMemberId serverId,
      ToLongFunction<RaftPeerId> commitInfoCache,
      Supplier<RetryCache.Statistics> retryCacheStatistics) {
    super(createRegistry(serverId.toString()));
    this.commitInfoCache = commitInfoCache;

    addPeerCommitIndexGauge(serverId.getPeerId());
    addRetryCacheMetric(retryCacheStatistics);
  }

  public LongCounter getNumRequestQueueLimitHits() {
    return numRequestQueueLimitHits;
  }

  public LongCounter getNumRequestsByteSizeLimitHits() {
    return numRequestsByteSizeLimitHits;
  }

  public LongCounter getNumResourceLimitHits() {
    return numResourceLimitHits;
  }

  public LongCounter getNumFailedClientStaleRead() {
    return numFailedClientStaleRead;
  }

  public LongCounter getNumInstallSnapshot() {
    return numInstallSnapshot;
  }

  public LongCounter getNumWatchRequestQueueLimitHits(ReplicationLevel replication) {
    return numWatchRequestQueueLimitHits.get(replication);
  }

  public LongCounter getNumWatchRequestsTimeout(ReplicationLevel replication) {
    return numWatchRequestsTimeout.get(replication);
  }

  private static RatlyMetricRegistry createRegistry(String serverId) {
    return create(new MetricRegistryInfo(serverId,
        RATLY_APPLICATION_NAME_METRICS, RATLY_SERVER_METRICS,
        RATLY_SERVER_METRICS_DESC));
  }

  private void addRetryCacheMetric(Supplier<RetryCache.Statistics> retryCacheStatistics) {
    getRegistry().gauge(RETRY_CACHE_ENTRY_COUNT_METRIC, () -> () -> retryCacheStatistics.get().size());
    getRegistry().gauge(RETRY_CACHE_HIT_COUNT_METRIC  , () -> () -> retryCacheStatistics.get().hitCount());
    getRegistry().gauge(RETRY_CACHE_HIT_RATE_METRIC   , () -> () -> retryCacheStatistics.get().hitRate());
    getRegistry().gauge(RETRY_CACHE_MISS_COUNT_METRIC , () -> () -> retryCacheStatistics.get().missCount());
    getRegistry().gauge(RETRY_CACHE_MISS_RATE_METRIC  , () -> () -> retryCacheStatistics.get().missRate());
  }

  /**
   * Register a follower with this Leader Metrics registry instance.
   */
  public void addFollower(RaftPeerId followerName) {
    String followerHbMetricKey = String.format(
        FOLLOWER_LAST_HEARTBEAT_ELAPSED_TIME_METRIC,
        followerName);

    followerLastHeartbeatElapsedTimeMap.put(followerName, 0L);
    getRegistry().gauge(followerHbMetricKey, () -> () -> followerLastHeartbeatElapsedTimeMap.get(followerName));

    addPeerCommitIndexGauge(followerName);
  }

  /**
   * Register a commit index tracker for the peer in cluster.
   */
  private void addPeerCommitIndexGauge(RaftPeerId peerId) {
    getRegistry().gauge(getPeerCommitIndexGaugeKey(peerId),
        () -> () -> commitInfoCache.applyAsLong(peerId));
  }

  @VisibleForTesting
  static RaftServerMetricsImpl getImpl(RaftGroupMemberId serverId) {
    return METRICS.get(serverId);
  }

  /**
   * Record heartbeat elapsed time for a follower within a Raft group.
   * @param followerId the follower id.
   * @param elapsedTimeNs Elapsed time in Nanos.
   */
  public void recordFollowerHeartbeatElapsedTime(RaftPeerId followerId, long elapsedTimeNs) {
    followerLastHeartbeatElapsedTimeMap.put(followerId, elapsedTimeNs);
  }

  public Timekeeper getFollowerAppendEntryTimer(boolean isHeartbeat) {
    return followerAppendEntryLatency.apply(isHeartbeat);
  }

  public Timekeeper getClientRequestTimer(Type request) {
    if (request.is(TypeCase.READ)) {
      return readTimer;
    } else if (request.is(TypeCase.STALEREAD)) {
      return staleReadTimer;
    } else if (request.is(TypeCase.WATCH)) {
      return watchTimers.get(request.getWatch().getReplication());
    } else if (request.is(TypeCase.WRITE)) {
      return writeTimer;
    }
    return null;
  }

  public void onRequestQueueLimitHit() {
    numRequestQueueLimitHits.inc();
  }

  public void addNumPendingRequestsGauge(Supplier<Integer> queueSize) {
    getRegistry().gauge(REQUEST_QUEUE_SIZE, () -> queueSize);
  }

  public boolean removeNumPendingRequestsGauge() {
    return getRegistry().remove(REQUEST_QUEUE_SIZE);
  }

  public void addNumPendingRequestsMegaByteSize(Supplier<Integer> megabyteSize) {
    getRegistry().gauge(REQUEST_MEGA_BYTE_SIZE, () -> megabyteSize);
  }

  public boolean removeNumPendingRequestsByteSize() {
    return getRegistry().remove(REQUEST_MEGA_BYTE_SIZE);
  }

  public void onWatchRequestQueueLimitHit(ReplicationLevel replicationLevel) {
    numWatchRequestQueueLimitHits.get(replicationLevel).inc();
  }

  public void onWatchRequestTimeout(ReplicationLevel replicationLevel) {
    numWatchRequestsTimeout.get(replicationLevel).inc();
  }

  public void addNumPendingWatchRequestsGauge(Supplier<Integer> queueSize, ReplicationLevel replication) {
    getRegistry().gauge(String.format(WATCH_REQUEST_QUEUE_SIZE, Type.toString(replication)), () -> queueSize);
  }

  public boolean removeNumPendingWatchRequestsGauge(ReplicationLevel replication) {
    return getRegistry().remove(String.format(WATCH_REQUEST_QUEUE_SIZE, Type.toString(replication)));
  }

  public void onRequestByteSizeLimitHit() {
    numRequestsByteSizeLimitHits.inc();
  }

  public void onResourceLimitHit() {
    numResourceLimitHits.inc();
  }

  void onFailedClientStaleRead() {
    numFailedClientStaleRead.inc();
  }

  void onFailedClientRead() {
    numFailedClientRead.inc();
  }

  void onFailedClientWatch() {
    numFailedClientWatch.inc();
  }

  void onFailedClientWrite() {
    numFailedClientWrite.inc();
  }

  void onFailedClientStream() {
    numFailedClientStream.inc();
  }

  public void incFailedRequestCount(Type type) {
    if (type.is(TypeCase.STALEREAD)) {
      onFailedClientStaleRead();
    } else if (type.is(TypeCase.WATCH)) {
      onFailedClientWatch();
    } else if (type.is(TypeCase.WRITE)) {
      onFailedClientWrite();
    } else if (type.is(TypeCase.READ)) {
      onFailedClientRead();
    } else if (type.is(TypeCase.MESSAGESTREAM)) {
      onFailedClientStream();
    }
  }

  @Override
  public void onSnapshotInstalled() {
    numInstallSnapshot.inc();
  }
}
