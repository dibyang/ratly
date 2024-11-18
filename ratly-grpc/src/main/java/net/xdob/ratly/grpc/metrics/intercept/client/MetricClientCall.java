
package net.xdob.ratly.grpc.metrics.intercept.client;

import io.grpc.ClientCall;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import net.xdob.ratly.grpc.metrics.MessageMetrics;

public class MetricClientCall<R, S> extends ForwardingClientCall.SimpleForwardingClientCall<R, S> {
  private final String metricNamePrefix;
  private final MessageMetrics metrics;

  public MetricClientCall(ClientCall<R, S> delegate,
                          MessageMetrics metrics,
                          String metricName){
    super(delegate);
    this.metricNamePrefix = metricName;
    this.metrics = metrics;
  }

  @Override
  public void start(ClientCall.Listener<S> delegate, Metadata metadata) {
    metrics.rpcStarted(metricNamePrefix);
    super.start(new MetricClientCallListener<>(
        delegate, metrics, metricNamePrefix), metadata);
  }
}
