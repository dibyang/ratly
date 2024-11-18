
package net.xdob.ratly.grpc.util;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * Invoke the given notifier when receiving a response.
 */
public class ResponseNotifyClientInterceptor implements ClientInterceptor {
  public static final Logger LOG = LoggerFactory.getLogger(ResponseNotifyClientInterceptor.class);

  private final Consumer<Object> notifier;

  public ResponseNotifyClientInterceptor(Consumer<Object> notifier) {
    this.notifier = notifier;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    LOG.debug("callOptions {}", callOptions);
    return new Call<>(next.newCall(method, callOptions));
  }

  private final class Call<ReqT, RespT>
      extends ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT> {

    private Call(ClientCall<ReqT, RespT> delegate) {
      super(delegate);
    }

    @Override
    public void start(Listener<RespT> responseListener, Metadata headers) {
      LOG.debug("start {}", headers);
      super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
        @Override
        public void onMessage(RespT message) {
          LOG.debug("onMessage {}", message);
          notifier.accept(message);
          super.onMessage(message);
        }
      }, headers);
    }
  }
}
