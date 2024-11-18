package net.xdob.ratly.grpc.client;

import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.grpc.GrpcTlsConfig;
import net.xdob.ratly.protocol.ClientId;
import io.grpc.stub.StreamObserver;
import net.xdob.ratly.proto.RaftProtos.RaftClientReplyProto;
import net.xdob.ratly.proto.RaftProtos.RaftClientRequestProto;
import net.xdob.ratly.protocol.RaftPeer;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Function;

public class GrpcClientProtocolProxy implements Closeable {
  private final GrpcClientProtocolClient proxy;
  private final Function<RaftPeer, CloseableStreamObserver> responseHandlerCreation;
  private RpcSession currentSession;

  public GrpcClientProtocolProxy(ClientId clientId, RaftPeer target,
      Function<RaftPeer, CloseableStreamObserver> responseHandlerCreation,
      RaftProperties properties, GrpcTlsConfig tlsConfig) {
    proxy = new GrpcClientProtocolClient(clientId, target, properties, tlsConfig, tlsConfig);
    this.responseHandlerCreation = responseHandlerCreation;
  }

  @Override
  public void close() throws IOException {
    closeCurrentSession();
    proxy.close();
  }

  @Override
  public String toString() {
    return "ProxyTo:" + proxy.getTarget();
  }

  public void closeCurrentSession() {
    if (currentSession != null) {
      currentSession.close();
      currentSession = null;
    }
  }

  public void onNext(RaftClientRequestProto request) {
    if (currentSession == null) {
      currentSession = new RpcSession(
          responseHandlerCreation.apply(proxy.getTarget()));
    }
    currentSession.requestObserver.onNext(request);
  }

  public void onError() {
    if (currentSession != null) {
      currentSession.onError();
    }
  }

  public interface CloseableStreamObserver
      extends StreamObserver<RaftClientReplyProto>, Closeable {
  }

  class RpcSession implements Closeable {
    private final StreamObserver<RaftClientRequestProto> requestObserver;
    private final CloseableStreamObserver responseHandler;
    private boolean hasError = false;

    RpcSession(CloseableStreamObserver responseHandler) {
      this.responseHandler = responseHandler;
      this.requestObserver = proxy.ordered(responseHandler);
    }

    void onError() {
      hasError = true;
    }

    @Override
    public void close() {
      if (!hasError) {
        try {
          requestObserver.onCompleted();
        } catch (Exception ignored) {
        }
      }
      try {
        responseHandler.close();
      } catch (IOException ignored) {
      }
    }
  }
}
