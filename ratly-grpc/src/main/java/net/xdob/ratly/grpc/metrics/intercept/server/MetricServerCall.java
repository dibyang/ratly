
package net.xdob.ratly.grpc.metrics.intercept.server;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.ForwardingServerCall;
import io.grpc.ServerCall;
import net.xdob.ratly.grpc.metrics.MessageMetrics;

class MetricServerCall<R,S> extends ForwardingServerCall.SimpleForwardingServerCall<R,S> {
  private final MessageMetrics metrics;
  private final String metricNamPrefix;

  MetricServerCall(ServerCall<R,S> delegate,
                       String metricNamPrefix,
                       MessageMetrics metrics){
    super(delegate);
    this.metricNamPrefix = metricNamPrefix;
    this.metrics = metrics;

    metrics.rpcStarted(metricNamPrefix);
  }

  @Override
  public void close(Status status, Metadata responseHeaders) {
    metrics.rpcCompleted(metricNamPrefix + "_" + status.getCode().toString());
    super.close(status, responseHeaders);
  }

}
