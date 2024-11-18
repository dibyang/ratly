
package net.xdob.ratly.client.impl;

import net.xdob.ratly.client.retry.ClientRetryEvent;
import net.xdob.ratly.client.RaftClientConfigKeys;
import net.xdob.ratly.client.impl.RaftClientImpl.PendingClientRequest;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.proto.RaftProtos.RaftClientRequestProto.TypeCase;
import net.xdob.ratly.proto.RaftProtos.SlidingWindowEntry;
import net.xdob.ratly.protocol.exceptions.AlreadyClosedException;
import net.xdob.ratly.protocol.exceptions.GroupMismatchException;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.exceptions.NotLeaderException;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftClientRequest;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.retry.RetryPolicy;
import net.xdob.ratly.rpc.CallId;
import net.xdob.ratly.util.BatchLogger;
import net.xdob.ratly.util.IOUtils;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.ProtoUtils;
import net.xdob.ratly.util.SlidingWindow;
import net.xdob.ratly.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;

/** Send ordered asynchronous requests to a raft service. */
public final class OrderedAsync {
  public static final Logger LOG = LoggerFactory.getLogger(OrderedAsync.class);

  private enum BatchLogKey implements BatchLogger.Key {
    SEND_REQUEST_EXCEPTION
  }

  static class PendingOrderedRequest extends PendingClientRequest
      implements SlidingWindow.ClientSideRequest<RaftClientReply> {
    private final long callId;
    private final long seqNum;
    private final AtomicReference<Function<SlidingWindowEntry, RaftClientRequest>> requestConstructor;
    private volatile boolean isFirst = false;

    PendingOrderedRequest(long callId, long seqNum,
        Function<SlidingWindowEntry, RaftClientRequest> requestConstructor) {
      this.callId = callId;
      this.seqNum = seqNum;
      this.requestConstructor = new AtomicReference<>(requestConstructor);
    }

    @Override
    public RaftClientRequest newRequestImpl() {
      return Optional.ofNullable(requestConstructor.get())
          .map(f -> f.apply(ProtoUtils.toSlidingWindowEntry(seqNum, isFirst)))
          .orElse(null);
    }

    @Override
    public void setFirstRequest() {
      isFirst = true;
    }

    @Override
    public long getSeqNum() {
      return seqNum;
    }

    @Override
    public boolean hasReply() {
      return getReplyFuture().isDone();
    }

    @Override
    public void setReply(RaftClientReply reply) {
      requestConstructor.set(null);
      getReplyFuture().complete(reply);
    }

    @Override
    public void fail(Throwable e) {
      requestConstructor.set(null);
      getReplyFuture().completeExceptionally(e);
    }

    @Override
    public String toString() {
      return "[cid=" + callId + ", seq=" + getSeqNum() + "]";
    }
  }

  static OrderedAsync newInstance(RaftClientImpl client, RaftProperties properties) {
    final OrderedAsync ordered = new OrderedAsync(client, properties);
    // send a dummy watch request to establish the connection
    // TODO: this is a work around, it is better to fix the underlying RPC implementation
    if (RaftClientConfigKeys.Async.Experimental.sendDummyRequest(properties)) {
      ordered.send(RaftClientRequest.watchRequestType(), null, null);
    }
    return ordered;
  }

  private final RaftClientImpl client;
  /** Map: id -> {@link SlidingWindow}, in order to support async calls to the Raft service or individual servers. */
  private final ConcurrentMap<String, SlidingWindow.Client<PendingOrderedRequest, RaftClientReply>> slidingWindows
      = new ConcurrentHashMap<>();
  private final Semaphore requestSemaphore;

  private OrderedAsync(RaftClientImpl client, RaftProperties properties) {
    this.client = Objects.requireNonNull(client, "client == null");
    this.requestSemaphore = new Semaphore(RaftClientConfigKeys.Async.outstandingRequestsMax(properties));
  }

  private void resetSlidingWindow(RaftClientRequest request) {
    getSlidingWindow(request).resetFirstSeqNum();
  }

  private SlidingWindow.Client<PendingOrderedRequest, RaftClientReply> getSlidingWindow(RaftClientRequest request) {
    return getSlidingWindow(request.isToLeader()? null: request.getServerId());
  }

  private SlidingWindow.Client<PendingOrderedRequest, RaftClientReply> getSlidingWindow(RaftPeerId target) {
    final String id = target != null ? target.toString() : "RAFT";
    return slidingWindows.computeIfAbsent(id, key -> new SlidingWindow.Client<>(client.getId() + "->" + key));
  }

  private void failAllAsyncRequests(RaftClientRequest request, Throwable t) {
    getSlidingWindow(request).fail(request.getSlidingWindowEntry().getSeqNum(), t);
  }

  CompletableFuture<RaftClientReply> send(RaftClientRequest.Type type, Message message, RaftPeerId server) {
    if (!type.is(TypeCase.WATCH) && !type.is(TypeCase.MESSAGESTREAM)) {
      Objects.requireNonNull(message, "message == null");
    }
    try {
      requestSemaphore.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return JavaUtils.completeExceptionally(IOUtils.toInterruptedIOException(
          "Interrupted when sending " + type + ", message=" + message, e));
    }

    final long callId = CallId.getAndIncrement();
    final LongFunction<PendingOrderedRequest> constructor = seqNum -> new PendingOrderedRequest(callId, seqNum,
        slidingWindowEntry -> client.newRaftClientRequest(server, callId, message, type, slidingWindowEntry));
    return getSlidingWindow(server).submitNewRequest(constructor, this::sendRequestWithRetry
    ).getReplyFuture(
    ).thenApply(reply -> RaftClientImpl.handleRaftException(reply, CompletionException::new)
    ).whenComplete((r, e) -> {
      if (e != null) {
        if (e.getCause() instanceof AlreadyClosedException) {
          LOG.error("Failed to send request, message=" + message + " due to " + e);
        } else {
          LOG.error("Failed to send request, message=" + message, e);
        }
      }
      requestSemaphore.release();
    });
  }

  private void sendRequestWithRetry(PendingOrderedRequest pending) {
    if (pending == null) {
      return;
    }
    if (pending.getReplyFuture().isDone()) {
      return;
    }

    final RaftClientRequest request = pending.newRequest();
    if (request == null) { // already done
      LOG.debug("{} newRequest returns null", pending);
      return;
    }

    if (getSlidingWindow((RaftPeerId) null).isFirst(pending.getSeqNum())) {
      pending.setFirstRequest();
    }
    LOG.debug("{}: send* {}", client.getId(), request);
    client.getClientRpc().sendRequestAsync(request).thenAccept(reply -> {
      LOG.debug("{}: receive* {}", client.getId(), reply);
      Objects.requireNonNull(reply, "reply == null");
      client.handleReply(request, reply);
      getSlidingWindow(request).receiveReply(
          request.getSlidingWindowEntry().getSeqNum(), reply, this::sendRequestWithRetry);
    }).exceptionally(e -> {
      final Throwable exception = e;
      final String key = client.getId() + "-" + request.getCallId() + "-" + exception;
      final Consumer<String> op = suffix -> LOG.error("{} {}: Failed* {}", suffix, client.getId(), request, exception);
      BatchLogger.warn(BatchLogKey.SEND_REQUEST_EXCEPTION, key, op);
      handleException(pending, request, e);
      return null;
    });
  }

  private void handleException(PendingOrderedRequest pending, RaftClientRequest request, Throwable e) {
    final RetryPolicy retryPolicy = client.getRetryPolicy();
    if (client.isClosed()) {
      failAllAsyncRequests(request, new AlreadyClosedException(client + " is closed."));
      return;
    }

    e = JavaUtils.unwrapCompletionException(e);
    if (!(e instanceof IOException) || e instanceof GroupMismatchException) {
      // non-retryable exceptions
      failAllAsyncRequests(request, e);
      return;
    }

    final ClientRetryEvent event = pending.newClientRetryEvent(request, e);
    final RetryPolicy.Action action = retryPolicy.handleAttemptFailure(event);
    if (!action.shouldRetry()) {
      failAllAsyncRequests(request, client.noMoreRetries(event));
      return;
    }

    if (e instanceof NotLeaderException) {
      client.handleNotLeaderException(request, (NotLeaderException) e, this::resetSlidingWindow);
    } else {
      client.handleIOException(request, (IOException) e, null, this::resetSlidingWindow);
    }
    final TimeDuration sleepTime = client.getEffectiveSleepTime(e, action.getSleepTime());
    LOG.debug("schedule* retry with sleep {} for attempt #{} of {}, {}",
        sleepTime, event.getAttemptCount(), request, retryPolicy);
    final SlidingWindow.Client<PendingOrderedRequest, RaftClientReply> slidingWindow = getSlidingWindow(request);
    client.getScheduler().onTimeout(sleepTime,
        () -> slidingWindow.retry(pending, this::sendRequestWithRetry),
        LOG, () -> "Failed* to retry " + pending);
  }

  void assertRequestSemaphore(int expectedAvailablePermits, int expectedQueueLength) {
    Preconditions.assertSame(expectedAvailablePermits, requestSemaphore.availablePermits(), "availablePermits");
    Preconditions.assertSame(expectedQueueLength, requestSemaphore.getQueueLength(), "queueLength");
  }
}
