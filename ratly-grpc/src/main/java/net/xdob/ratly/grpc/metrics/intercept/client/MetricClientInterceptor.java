
package net.xdob.ratly.grpc.metrics.intercept.client;

import net.xdob.ratly.grpc.metrics.MessageMetrics;
import io.grpc.*;

import java.io.Closeable;

/**
 * An implementation of a client interceptor.
 * Intercepts the messages and increments metrics accordingly
 * before sending them.
 */

public class MetricClientInterceptor implements ClientInterceptor, Closeable {
  private final String identifier;
  private final MessageMetrics metrics;

  public MetricClientInterceptor(String identifier){
    this.identifier = identifier;
    this.metrics = new MessageMetrics(identifier, "client");
  }

  private String getMethodMetricPrefix(MethodDescriptor<?, ?> method){
    String serviceName = MethodDescriptor.extractFullServiceName(method.getFullMethodName());
    String methodName = method.getFullMethodName().substring(serviceName.length() + 1);
    return identifier + "_" + serviceName + "_" + methodName;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> methodDescriptor,
                                                             CallOptions callOptions,
                                                             Channel channel) {

    return new MetricClientCall<>(
        channel.newCall(methodDescriptor, callOptions),
        metrics,
        getMethodMetricPrefix(methodDescriptor)
    );
  }

  @Override
  public void close() {
    metrics.unregister();
  }
}
