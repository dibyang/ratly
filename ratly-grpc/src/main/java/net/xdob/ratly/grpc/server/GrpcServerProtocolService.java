
package net.xdob.ratly.grpc.server;

import java.util.function.Consumer;
import java.util.function.Function;
import net.xdob.ratly.grpc.GrpcUtil;
import net.xdob.ratly.grpc.metrics.ZeroCopyMetrics;
import net.xdob.ratly.grpc.util.ZeroCopyMessageMarshaller;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.protocol.RaftServerProtocol;
import net.xdob.ratly.server.util.ServerStringUtils;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.xdob.ratly.proto.raft.*;
import net.xdob.ratly.proto.grpc.RaftServerProtocolServiceGrpc.RaftServerProtocolServiceImplBase;
import net.xdob.ratly.util.ProtoUtils;
import net.xdob.ratly.util.ReferenceCountedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static net.xdob.ratly.grpc.GrpcUtil.addMethodWithCustomMarshaller;
import static net.xdob.ratly.proto.grpc.RaftServerProtocolServiceGrpc.getAppendEntriesMethod;

/**
 * Followers 通过 GrpcServerProtocolService 接收日志，校验后写入本地日志
 */
class GrpcServerProtocolService extends RaftServerProtocolServiceImplBase {
  public static final Logger LOG = LoggerFactory.getLogger(GrpcServerProtocolService.class);

  static class PendingServerRequest<REQUEST> {
    private final AtomicReference<ReferenceCountedObject<REQUEST>> requestRef;
    private final CompletableFuture<Void> future = new CompletableFuture<>();

    PendingServerRequest(ReferenceCountedObject<REQUEST> requestRef) {
      requestRef.retain();
      this.requestRef = new AtomicReference<>(requestRef);
    }

    REQUEST getRequest() {
      return Optional.ofNullable(requestRef.get())
          .map(ReferenceCountedObject::get)
          .orElse(null);
    }

    CompletableFuture<Void> getFuture() {
      return future;
    }

    void release() {
      Optional.ofNullable(requestRef.getAndSet(null))
          .ifPresent(ReferenceCountedObject::release);
    }
  }

  abstract class ServerRequestStreamObserver<REQUEST, REPLY> implements StreamObserver<REQUEST> {
    private final RaftServer.Op op;
    private final StreamObserver<REPLY> responseObserver;
    /** For ordered {@link #onNext(Object)} requests. */
    private final AtomicReference<PendingServerRequest<REQUEST>> previousOnNext = new AtomicReference<>();
    /** For both ordered and unordered {@link #onNext(Object)} requests. */
    private final AtomicReference<CompletableFuture<REPLY>> requestFuture
        = new AtomicReference<>(CompletableFuture.completedFuture(null));
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    ServerRequestStreamObserver(RaftServer.Op op, StreamObserver<REPLY> responseObserver) {
      this.op = op;
      this.responseObserver = responseObserver;
    }

    private String getPreviousRequestString() {
      return Optional.ofNullable(previousOnNext.get())
          .map(PendingServerRequest::getRequest)
          .map(this::requestToString)
          .orElse(null);
    }

    CompletableFuture<REPLY> process(REQUEST request) throws IOException {
      throw new UnsupportedOperationException("This method is not supported.");
    }

    CompletableFuture<REPLY> process(ReferenceCountedObject<REQUEST> requestRef)
        throws IOException {
      try {
        return process(requestRef.retain());
      } finally {
        requestRef.release();
      }
    }

    void release(REQUEST req) {
    }

    abstract long getCallId(REQUEST request);

    boolean isHeartbeat(REQUEST request) {
      return false;
    }

    abstract String requestToString(REQUEST request);

    abstract String replyToString(REPLY reply);

    abstract boolean replyInOrder(REQUEST request);

    private synchronized void handleError(Throwable e, long callId, boolean isHeartbeat) {
      GrpcUtil.warn(LOG, () -> getId() + ": Failed " + op + " request cid=" + callId + ", isHeartbeat? "
          + isHeartbeat, e);
      if (isClosed.compareAndSet(false, true)) {
        responseObserver.onError(GrpcUtil.wrapException(e, callId, isHeartbeat));
      }
    }

    private synchronized REPLY handleReply(REPLY reply) {
      if (!isClosed.get()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: reply {}", getId(), replyToString(reply));
        }
        responseObserver.onNext(reply);
      }
      return reply;
    }

    void composeRequest(CompletableFuture<REPLY> current) {
      requestFuture.updateAndGet(previous -> previous.thenCompose(reply -> current));
    }

    @Override
    public void onNext(REQUEST request) {
      ReferenceCountedObject<REQUEST> requestRef = ReferenceCountedObject.wrap(request, () -> {}, released -> {
        if (released) {
          release(request);
        }
      });

      if (!replyInOrder(request)) {
        try {
          composeRequest(process(requestRef).thenApply(this::handleReply));
        } catch (Exception e) {
          handleError(e, getCallId(request), isHeartbeat(request));
          release(request);
        }
        return;
      }

      final PendingServerRequest<REQUEST> current = new PendingServerRequest<>(requestRef);
      final long callId = getCallId(current.getRequest());
      final boolean isHeartbeat = isHeartbeat(current.getRequest());
      final Optional<PendingServerRequest<REQUEST>> previous = Optional.ofNullable(previousOnNext.getAndSet(current));
      final CompletableFuture<Void> previousFuture = previous.map(PendingServerRequest::getFuture)
          .orElse(CompletableFuture.completedFuture(null));
      try {
        final CompletableFuture<REPLY> f = process(requestRef).exceptionally(e -> {
          // Handle cases, such as RaftServer is paused
          handleError(e, callId, isHeartbeat);
          current.getFuture().completeExceptionally(e);
          return null;
        }).thenCombine(previousFuture, (reply, v) -> {
          handleReply(reply);
          current.getFuture().complete(null);
          return null;
        });
        composeRequest(f);
      } catch (Exception e) {
        handleError(e, callId, isHeartbeat);
        current.getFuture().completeExceptionally(e);
      } finally {
        previous.ifPresent(PendingServerRequest::release);
        if (isClosed.get()) {
          // Some requests may come after onCompleted or onError, ensure they're released.
          releaseLast();
        }
      }
    }

    @Override
    public void onCompleted() {
      if (isClosed.compareAndSet(false, true)) {
        LOG.info("{}: Completed {}, lastRequest: {}", getId(), op, getPreviousRequestString());
        requestFuture.get().thenAccept(reply -> {
          LOG.info("{}: Completed {}, lastReply: {}", getId(), op, reply);
          responseObserver.onCompleted();
        });
        releaseLast();
      }
    }

    @Override
    public void onError(Throwable t) {
      GrpcUtil.warn(LOG, () -> getId() + ": "+ op + " onError, lastRequest: " + getPreviousRequestString(), t);
      if (isClosed.compareAndSet(false, true)) {
        Status status = Status.fromThrowable(t);
        if (status != null && status.getCode() != Status.Code.CANCELLED) {
          responseObserver.onCompleted();
        }
        releaseLast();
      }
    }

    private void releaseLast() {
      Optional.ofNullable(previousOnNext.get()).ifPresent(PendingServerRequest::release);
    }
  }

  private final Supplier<RaftPeerId> idSupplier;
  private final RaftServer server;
  private final boolean zeroCopyEnabled;
  private final ZeroCopyMessageMarshaller<AppendEntriesRequestProto> zeroCopyRequestMarshaller;

  GrpcServerProtocolService(Supplier<RaftPeerId> idSupplier, RaftServer server, boolean zeroCopyEnabled,
      ZeroCopyMetrics zeroCopyMetrics) {
    this.idSupplier = idSupplier;
    this.server = server;
    this.zeroCopyEnabled = zeroCopyEnabled;
    this.zeroCopyRequestMarshaller = new ZeroCopyMessageMarshaller<>(AppendEntriesRequestProto.getDefaultInstance(),
        zeroCopyMetrics::onZeroCopyMessage, zeroCopyMetrics::onNonZeroCopyMessage, zeroCopyMetrics::onReleasedMessage);
    zeroCopyMetrics.addUnreleased("server_protocol", zeroCopyRequestMarshaller::getUnclosedCount);
  }

  RaftPeerId getId() {
    return idSupplier.get();
  }

  ServerServiceDefinition bindServiceWithZeroCopy() {
    ServerServiceDefinition orig = super.bindService();
    if (!zeroCopyEnabled) {
      LOG.info("{}: Zero copy is disabled.", getId());
      return orig;
    }
    ServerServiceDefinition.Builder builder = ServerServiceDefinition.builder(orig.getServiceDescriptor().getName());

    // Add appendEntries with zero copy marshaller.
    addMethodWithCustomMarshaller(orig, builder, getAppendEntriesMethod(), zeroCopyRequestMarshaller);
    // Add remaining methods as is.
    orig.getMethods().stream().filter(
        x -> !x.getMethodDescriptor().getFullMethodName().equals(getAppendEntriesMethod().getFullMethodName())
    ).forEach(
        builder::addMethod
    );

    return builder.build();
  }

  @Override
  public void requestVote(RequestVoteRequestProto request,
      StreamObserver<RequestVoteReplyProto> responseObserver) {
    try {
      final RequestVoteReplyProto reply = server.requestVote(request);
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (Exception e) {
      GrpcUtil.warn(LOG, () -> getId() + ": Failed requestVote " + ProtoUtils.toString(request.getServerRequest()), e);
      responseObserver.onError(GrpcUtil.wrapException(e));
    }
  }

  @Override
  public void startLeaderElection(StartLeaderElectionRequestProto request,
      StreamObserver<StartLeaderElectionReplyProto> responseObserver) {
    try {
      final StartLeaderElectionReplyProto reply = server.startLeaderElection(request);
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    } catch (Throwable e) {
      GrpcUtil.warn(LOG,
          () -> getId() + ": Failed startLeaderElection " + ProtoUtils.toString(request.getServerRequest()), e);
      responseObserver.onError(GrpcUtil.wrapException(e));
    }
  }

  @Override
  public void readIndex(ReadIndexRequestProto request, StreamObserver<ReadIndexReplyProto> responseObserver) {
    final Consumer<Throwable> warning = e -> GrpcUtil.warn(LOG,
            () -> getId() + ": Failed readIndex " + ProtoUtils.toString(request.getServerRequest()), e);
    GrpcUtil.asyncCall(responseObserver, () -> server.readIndexAsync(request), Function.identity(), warning);
  }

  @Override
  public StreamObserver<AppendEntriesRequestProto> appendEntries(
      StreamObserver<AppendEntriesReplyProto> responseObserver) {
    return new ServerRequestStreamObserver<AppendEntriesRequestProto, AppendEntriesReplyProto>(
        RaftServerProtocol.Op.APPEND_ENTRIES, responseObserver) {
      @Override
      CompletableFuture<AppendEntriesReplyProto> process(ReferenceCountedObject<AppendEntriesRequestProto> requestRef)
          throws IOException {
        return server.appendEntriesAsync(requestRef);
      }

      @Override
      void release(AppendEntriesRequestProto req) {
        zeroCopyRequestMarshaller.release(req);
      }

      @Override
      long getCallId(AppendEntriesRequestProto request) {
        return request.getServerRequest().getCallId();
      }

      @Override
      boolean isHeartbeat(AppendEntriesRequestProto request) {
        return request.getEntriesCount() == 0;
      }

      @Override
      String requestToString(AppendEntriesRequestProto request) {
        return ServerStringUtils.toAppendEntriesRequestString(request, null);
      }

      @Override
      String replyToString(AppendEntriesReplyProto reply) {
        return ServerStringUtils.toAppendEntriesReplyString(reply);
      }

      @Override
      boolean replyInOrder(AppendEntriesRequestProto request) {
        return request.getEntriesCount() != 0;
      }
    };
  }

  @Override
  public StreamObserver<InstallSnapshotRequestProto> installSnapshot(
      StreamObserver<InstallSnapshotReplyProto> responseObserver) {
    return new ServerRequestStreamObserver<InstallSnapshotRequestProto, InstallSnapshotReplyProto>(
        RaftServerProtocol.Op.INSTALL_SNAPSHOT, responseObserver) {
      @Override
      CompletableFuture<InstallSnapshotReplyProto> process(InstallSnapshotRequestProto request) throws IOException {
        return CompletableFuture.completedFuture(server.installSnapshot(request));
      }

      @Override
      long getCallId(InstallSnapshotRequestProto request) {
        return request.getServerRequest().getCallId();
      }

      @Override
      String requestToString(InstallSnapshotRequestProto request) {
        return ServerStringUtils.toInstallSnapshotRequestString(request);
      }

      @Override
      String replyToString(InstallSnapshotReplyProto reply) {
        return ServerStringUtils.toInstallSnapshotReplyString(reply);
      }

      @Override
      boolean replyInOrder(InstallSnapshotRequestProto installSnapshotRequestProto) {
        return true;
      }
    };
  }
}
