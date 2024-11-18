package net.xdob.ratly.grpc.metrics.intercept.client;

import io.grpc.ClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.Status;
import net.xdob.ratly.grpc.metrics.MessageMetrics;

public class MetricClientCallListener<S> extends ForwardingClientCallListener<S> {
  private final String metricNamePrefix;
  private final MessageMetrics metrics;
  private final ClientCall.Listener<S> delegate;

  MetricClientCallListener(ClientCall.Listener<S> delegate,
                           MessageMetrics metrics,
                           String metricNamePrefix){
    this.delegate = delegate;
    this.metricNamePrefix = metricNamePrefix;
    this.metrics = metrics;
  }

  @Override
  protected ClientCall.Listener<S> delegate() {
    return delegate;
  }

  @Override
  public void onClose(Status status, Metadata metadata) {
    metrics.rpcReceived(metricNamePrefix + "_" + status.getCode().toString());
    super.onClose(status, metadata);
  }
}
