
package net.xdob.ratly.grpc.metrics.intercept.server;

import net.xdob.ratly.protocol.RaftPeerId;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import net.xdob.ratly.grpc.metrics.MessageMetrics;
import io.grpc.ServerInterceptor;

import java.io.Closeable;
import java.util.function.Supplier;

/**
 * An implementation of a server interceptor.
 * Intercepts the inbound/outbound messages and increments metrics accordingly
 * before handling them.
 */

public class MetricServerInterceptor implements ServerInterceptor, Closeable {
  private String identifier;
  private MessageMetrics metrics;
  private final Supplier<RaftPeerId> peerIdSupplier;
  private final String defaultIdentifier;

  public MessageMetrics getMetrics() {
    return metrics;
  }

  public MetricServerInterceptor(Supplier<RaftPeerId> idSupplier, String defaultIdentifier){
    this.peerIdSupplier = idSupplier;
    this.identifier = null;
    this.defaultIdentifier = defaultIdentifier;
  }

  private String getMethodMetricPrefix(MethodDescriptor<?, ?> method){
    String serviceName = MethodDescriptor.extractFullServiceName(method.getFullMethodName());
    String methodName = method.getFullMethodName().substring(serviceName.length() + 1);
    return identifier + "_" + serviceName + "_" + methodName;
  }

  @Override
  public <R, S> ServerCall.Listener<R> interceptCall(
      ServerCall<R, S> call,
      Metadata requestHeaders,
      ServerCallHandler<R, S> next) {
    MethodDescriptor<R, S> method = call.getMethodDescriptor();
    if (identifier == null) {
      try {
        identifier = peerIdSupplier.get().toString();
      } catch (Exception e) {
        identifier = defaultIdentifier;
      }
    }
    if (metrics == null) {
      metrics = new MessageMetrics(identifier, "server");
    }
    String metricNamePrefix = getMethodMetricPrefix(method);
    ServerCall<R,S> monitoringCall = new MetricServerCall<>(call, metricNamePrefix, metrics);
    return next.startCall(monitoringCall, requestHeaders);
  }

  @Override
  public void close() {
    final MessageMetrics m = metrics;
    if (m != null) {
      m.unregister();
    }
  }
}
