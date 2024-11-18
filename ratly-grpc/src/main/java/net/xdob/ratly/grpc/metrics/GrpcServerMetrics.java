package net.xdob.ratly.grpc.metrics;

import net.xdob.ratly.metrics.LongCounter;
import net.xdob.ratly.metrics.MetricRegistryInfo;
import net.xdob.ratly.metrics.RatlyMetricRegistry;
import net.xdob.ratly.metrics.RatlyMetrics;

import net.xdob.ratly.metrics.Timekeeper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

public class GrpcServerMetrics extends RatlyMetrics {
  private static final String RATLY_GRPC_METRICS_APP_NAME = "ratly_grpc";
  private static final String RATLY_GRPC_METRICS_COMP_NAME = "log_appender";
  private static final String RATLY_GRPC_METRICS_DESC = "Metrics for Ratly Grpc Log Appender";

  public static final String RATLY_GRPC_METRICS_LOG_APPENDER_LATENCY =
      "%s_latency";
  public static final String RATLY_GRPC_METRICS_LOG_APPENDER_SUCCESS =
      "%s_success_reply_count";
  public static final String RATLY_GRPC_METRICS_LOG_APPENDER_NOT_LEADER =
      "%s_not_leader_reply_count";
  public static final String RATLY_GRPC_METRICS_LOG_APPENDER_INCONSISTENCY =
      "%s_inconsistency_reply_count";
  public static final String RATLY_GRPC_METRICS_LOG_APPENDER_TIMEOUT =
      "%s_append_entry_timeout_count";
  public static final String RATLY_GRPC_METRICS_LOG_APPENDER_PENDING_COUNT
      = "%s_pending_log_requests_count";

  public static final String RATLY_GRPC_METRICS_REQUEST_RETRY_COUNT = "num_retries";
  public static final String RATLY_GRPC_METRICS_REQUESTS_COUNT = "num_requests";
  public static final String RATLY_GRPC_INSTALL_SNAPSHOT_COUNT = "num_install_snapshot";

  private final LongCounter requestRetry = getRegistry().counter(RATLY_GRPC_METRICS_REQUEST_RETRY_COUNT);
  private final LongCounter requestInstallSnapshot = getRegistry().counter(RATLY_GRPC_INSTALL_SNAPSHOT_COUNT);

  private final Function<Boolean, LongCounter> requestCreate = newHeartbeatCounter(RATLY_GRPC_METRICS_REQUESTS_COUNT);

  private final Map<String, Function<Boolean, LongCounter>> requestSuccess = new ConcurrentHashMap<>();
  private final Map<String, Function<Boolean, LongCounter>> requestTimeout = new ConcurrentHashMap<>();

  private final Map<String, LongCounter> requestNotLeader = new ConcurrentHashMap<>();
  private final Map<String, LongCounter> requestInconsistency = new ConcurrentHashMap<>();

  private final Map<String, String> heartbeatLatency = new ConcurrentHashMap<>();
  private final Map<String, String> appendLogLatency = new ConcurrentHashMap<>();

  public GrpcServerMetrics(String serverId) {
    super(createRegistry(serverId));
  }

  private static RatlyMetricRegistry createRegistry(String serverId) {
    return create(new MetricRegistryInfo(serverId,
        RATLY_GRPC_METRICS_APP_NAME,
        RATLY_GRPC_METRICS_COMP_NAME, RATLY_GRPC_METRICS_DESC));
  }

  public Timekeeper getGrpcLogAppenderLatencyTimer(String follower, boolean isHeartbeat) {
    final Map<String, String> map = isHeartbeat ? heartbeatLatency : appendLogLatency;
    final String name = map.computeIfAbsent(follower,
        key -> String.format(RATLY_GRPC_METRICS_LOG_APPENDER_LATENCY + getHeartbeatSuffix(isHeartbeat), key));
    return getRegistry().timer(name);
  }

  public void onRequestRetry() {
    requestRetry.inc();
  }

  public void onRequestCreate(boolean isHeartbeat) {
    requestCreate.apply(isHeartbeat).inc();
  }

  private Function<Boolean, LongCounter> newRequestSuccess(String follower) {
    final String prefix = String.format(RATLY_GRPC_METRICS_LOG_APPENDER_SUCCESS, follower);
    return newHeartbeatCounter(prefix);
  }

  public void onRequestSuccess(String follower, boolean isHeartbeat) {
    requestSuccess.computeIfAbsent(follower, this::newRequestSuccess).apply(isHeartbeat).inc();
  }

  private LongCounter newRequestNotLeader(String follower) {
    return getRegistry().counter(String.format(RATLY_GRPC_METRICS_LOG_APPENDER_NOT_LEADER, follower));
  }

  public void onRequestNotLeader(String follower) {
    requestNotLeader.computeIfAbsent(follower, this::newRequestNotLeader).inc();
  }

  private LongCounter newRequestInconsistency(String follower) {
    return getRegistry().counter(String.format(RATLY_GRPC_METRICS_LOG_APPENDER_INCONSISTENCY, follower));
  }

  public void onRequestInconsistency(String follower) {
    requestInconsistency.computeIfAbsent(follower, this::newRequestInconsistency).inc();
  }

  private Function<Boolean, LongCounter> newRequestTimeout(String follower) {
    final String prefix = String.format(RATLY_GRPC_METRICS_LOG_APPENDER_TIMEOUT, follower);
    return newHeartbeatCounter(prefix);
  }

  public void onRequestTimeout(String follower, boolean isHeartbeat) {
    requestTimeout.computeIfAbsent(follower, this::newRequestTimeout).apply(isHeartbeat).inc();
  }

  public void addPendingRequestsCount(String follower, Supplier<Integer> pendinglogQueueSize) {
    final String name = String.format(RATLY_GRPC_METRICS_LOG_APPENDER_PENDING_COUNT, follower);
    getRegistry().gauge(name, () -> pendinglogQueueSize);
  }

  public void onInstallSnapshot() {
    requestInstallSnapshot.inc();
  }
}
