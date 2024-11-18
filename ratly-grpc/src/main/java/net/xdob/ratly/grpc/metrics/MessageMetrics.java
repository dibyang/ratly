
package net.xdob.ratly.grpc.metrics;

import net.xdob.ratly.metrics.LongCounter;
import net.xdob.ratly.metrics.MetricRegistryInfo;
import net.xdob.ratly.metrics.RatlyMetricRegistry;
import net.xdob.ratly.metrics.RatlyMetrics;

import java.util.Map;

public class MessageMetrics extends RatlyMetrics {
  public static final String GRPC_MESSAGE_METRICS = "%s_message_metrics";
  public static final String GRPC_MESSAGE_METRICS_DESC = "Outbound/Inbound message counters";

  private enum Type {
    STARTED("_started_total"),
    COMPLETED("_completed_total"),
    RECEIVED("_received_executed");

    private final String suffix;

    Type(String suffix) {
      this.suffix = suffix;
    }

    String getSuffix() {
      return suffix;
    }
  }

  private final Map<Type, Map<String, LongCounter>> types;

  public MessageMetrics(String endpointId, String endpointType) {
    super(createRegistry(endpointId, endpointType));
    this.types = newCounterMaps(Type.class);
  }

  private static RatlyMetricRegistry createRegistry(String endpointId, String endpointType) {
    final String name = String.format(GRPC_MESSAGE_METRICS, endpointType);
    return create(new MetricRegistryInfo(endpointId,
        RATLY_APPLICATION_NAME_METRICS, name, GRPC_MESSAGE_METRICS_DESC));
  }

  private void inc(String metricNamePrefix, Type t) {
    types.get(t)
        .computeIfAbsent(metricNamePrefix, prefix -> getRegistry().counter(prefix + t.getSuffix()))
        .inc();
    final Map<String, LongCounter> counters = types.get(t);
    LongCounter c = counters.get(metricNamePrefix);
    if (c == null) {
      synchronized (counters) {
        c = counters.computeIfAbsent(metricNamePrefix, prefix -> getRegistry().counter(prefix + t.getSuffix()));
      }
    }
    c.inc();
  }

  /**
   * Increments the count of RPCs that are started.
   * Both client and server use this.
   */
  public void rpcStarted(String metricNamePrefix){
    inc(metricNamePrefix, Type.STARTED);
  }

  /**
   * Increments the count of RPCs that were started and got completed.
   * Both client and server use this.
   */
  public void rpcCompleted(String metricNamePrefix){
    inc(metricNamePrefix, Type.COMPLETED);
  }

  /**
   * Increments the count of RPCs received on the server.
   */
  public void rpcReceived(String metricNamePrefix){
    inc(metricNamePrefix, Type.RECEIVED);
  }
}
