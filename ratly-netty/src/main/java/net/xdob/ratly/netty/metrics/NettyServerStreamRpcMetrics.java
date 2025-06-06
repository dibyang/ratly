package net.xdob.ratly.netty.metrics;

import net.xdob.ratly.metrics.LongCounter;
import net.xdob.ratly.metrics.MetricRegistryInfo;
import net.xdob.ratly.metrics.RatlyMetricRegistry;
import net.xdob.ratly.metrics.RatlyMetrics;
import net.xdob.ratly.metrics.Timekeeper;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class NettyServerStreamRpcMetrics extends RatlyMetrics {
  private static final String METRICS_APP_NAME = "ratly_netty";
  private static final String METRICS_COMP_NAME = "stream_server";
  private static final String METRICS_DESC = "Metrics for Ratly Netty Stream Server";

  private static final String METRICS_LATENCY = "%s_latency";
  private static final String METRICS_SUCCESS = "%s_success_reply_count";
  private static final String METRICS_FAIL = "%s_fail_reply_count";
  private static final String METRICS_NUM_REQUESTS = "num_requests_%s";

  public enum RequestType {
    CHANNEL_READ, HEADER, LOCAL_WRITE, REMOTE_WRITE, STATE_MACHINE_STREAM, START_TRANSACTION;

    private final String numRequestsString;
    private final String successCountString;
    private final String failCountString;
    private final String latencyString;

    RequestType() {
      final String lower = name().toLowerCase(Locale.ENGLISH);
      this.numRequestsString = String.format(METRICS_NUM_REQUESTS, lower);
      this.successCountString = String.format(METRICS_SUCCESS, lower);
      this.failCountString = String.format(METRICS_FAIL, lower);
      this.latencyString = String.format(METRICS_LATENCY, lower);
    }

    String getNumRequestsString() {
      return numRequestsString;
    }
    String getSuccessCountString() {
      return successCountString;
    }
    String getFailCountString() {
      return failCountString;
    }
    String getLatencyString() {
      return latencyString;
    }
  }

  public final class RequestMetrics {
    private final RequestType type;
    private final Timekeeper timer;

    private RequestMetrics(RequestType type) {
      this.type = type;
      this.timer = getLatencyTimer(type);
    }

    public Timekeeper.Context start() {
      onRequestCreate(type);
      return timer.time();
    }

    public void stop(Timekeeper.Context context, boolean success) {
      context.stop();
      if (success) {
        onRequestSuccess(type);
      } else {
        onRequestFail(type);
      }
    }
  }

  private enum Op {
    Create(RequestType::getNumRequestsString),
    Success(RequestType::getSuccessCountString),
    Fail(RequestType::getFailCountString);

    private final Function<RequestType, String> stringFunction;

    Op(Function<RequestType, String> stringFunction) {
      this.stringFunction = stringFunction;
    }

    String getString(RequestType type) {
      return stringFunction.apply(type);
    }
  }

  private final Map<String, Timekeeper> latencyTimers = new ConcurrentHashMap<>();
  private final Map<Op, Map<String, LongCounter>> ops;

  public NettyServerStreamRpcMetrics(String serverId) {
    super(createRegistry(serverId));

    this.ops = newCounterMaps(Op.class);
  }

  private static RatlyMetricRegistry createRegistry(String serverId) {
    return create(new MetricRegistryInfo(serverId,
        METRICS_APP_NAME, METRICS_COMP_NAME, METRICS_DESC));
  }

  public RequestMetrics newRequestMetrics(RequestType type) {
    return new RequestMetrics(type);
  }

  public Timekeeper getLatencyTimer(RequestType type) {
    return latencyTimers.computeIfAbsent(type.getLatencyString(), getRegistry()::timer);
  }

  private void inc(Op op, RequestType type) {
    ops.get(op).computeIfAbsent(op.getString(type), getRegistry()::counter).inc();
  }

  public void onRequestCreate(RequestType type) {
    inc(Op.Create, type);
  }

  public void onRequestSuccess(RequestType type) {
    inc(Op.Success, type);
  }

  public void onRequestFail(RequestType type) {
    inc(Op.Fail, type);
  }
}
