
package net.xdob.ratly.client.impl;

import net.xdob.ratly.client.retry.ClientRetryEvent;
import net.xdob.ratly.client.impl.RaftClientImpl.PendingClientRequest;
import net.xdob.ratly.protocol.ClientId;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.protocol.exceptions.AlreadyClosedException;
import net.xdob.ratly.protocol.exceptions.GroupMismatchException;
import net.xdob.ratly.protocol.exceptions.NotLeaderException;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftClientRequest;
import net.xdob.ratly.protocol.exceptions.RaftException;
import net.xdob.ratly.retry.RetryPolicy;
import net.xdob.ratly.rpc.CallId;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

/** Send unordered asynchronous requests to a raft service. */
public interface UnorderedAsync {
  Logger LOG = LoggerFactory.getLogger(UnorderedAsync.class);

  class PendingUnorderedRequest extends PendingClientRequest {
    private final Supplier<RaftClientRequest> requestConstructor;

    PendingUnorderedRequest(Supplier<RaftClientRequest> requestConstructor) {
      this.requestConstructor = requestConstructor;
    }

    @Override
    public RaftClientRequest newRequestImpl() {
      return requestConstructor.get();
    }
  }

  static CompletableFuture<RaftClientReply> send(RaftClientRequest.Type type, Message message, RaftPeerId server,
      RaftClientImpl client) {
    final long callId = CallId.getAndIncrement();
    final PendingClientRequest pending = new PendingUnorderedRequest(
        () -> client.newRaftClientRequest(server, callId, message, type, null));
    sendRequestWithRetry(pending, client);
    return pending.getReplyFuture()
        .thenApply(reply -> RaftClientImpl.handleRaftException(reply, CompletionException::new));
  }

  static void sendRequestWithRetry(PendingClientRequest pending, RaftClientImpl client) {
    final CompletableFuture<RaftClientReply> f = pending.getReplyFuture();
    if (f.isDone()) {
      return;
    }

    final RaftClientRequest request = pending.newRequest();
    final int attemptCount = pending.getAttemptCount();

    final ClientId clientId = client.getId();
    LOG.debug("{}: attempt #{} send~ {}", clientId, attemptCount, request);
    client.getClientRpc().sendRequestAsyncUnordered(request).whenCompleteAsync((reply, e) -> {
      try {
        LOG.debug("{}: attempt #{} receive~ {}", clientId, attemptCount, reply);
        final RaftException replyException = reply != null? reply.getException(): null;
        reply = client.handleLeaderException(request, reply);
        if (reply != null) {
          client.handleReply(request, reply);
          f.complete(reply);
          return;
        }

        final Throwable cause = replyException != null ? replyException : e;
        if (client.isClosed()) {
          f.completeExceptionally(new AlreadyClosedException(client + " is closed"));
          return;
        }

        final ClientRetryEvent event = pending.newClientRetryEvent(request, cause);
        RetryPolicy retryPolicy = client.getRetryPolicy();
        final RetryPolicy.Action action = retryPolicy.handleAttemptFailure(event);
        if (!action.shouldRetry()) {
          f.completeExceptionally(client.noMoreRetries(event));
          return;
        }

        if (e != null) {
          if (LOG.isTraceEnabled()) {
            LOG.trace(clientId + ": attempt #" + attemptCount + " failed~ " + request, e);
          } else {
            LOG.debug("{}: attempt #{} failed {} with {}", clientId, attemptCount, request, e.toString());
          }
          e = JavaUtils.unwrapCompletionException(e);

          if (e instanceof IOException) {
            if (e instanceof NotLeaderException) {
              client.handleNotLeaderException(request, (NotLeaderException) e, null);
            } else if (e instanceof GroupMismatchException) {
              f.completeExceptionally(e);
              return;
            } else {
              client.handleIOException(request, (IOException) e);
            }
          } else {
            if (!client.getClientRpc().handleException(request.getServerId(), e, false)) {
              f.completeExceptionally(e);
              return;
            }
          }
        }

        final TimeDuration sleepTime = client.getEffectiveSleepTime(cause, action.getSleepTime());
        LOG.debug("schedule~ attempt #{} with sleep {} and policy {} for {}",
            attemptCount, sleepTime, retryPolicy, request);
        client.getScheduler().onTimeout(sleepTime,
            () -> sendRequestWithRetry(pending, client), LOG, () -> clientId + ": Failed~ to retry " + request);
      } catch (Exception ex) {
        LOG.error(clientId + ": Failed " + request, ex);
        f.completeExceptionally(ex);
      }
    });
  }
}
