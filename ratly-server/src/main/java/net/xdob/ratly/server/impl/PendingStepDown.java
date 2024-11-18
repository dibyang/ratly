
package net.xdob.ratly.server.impl;

import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.TransferLeadershipRequest;
import net.xdob.ratly.protocol.exceptions.TimeoutIOException;
import net.xdob.ratly.server.leader.LeaderState;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.MemoizedSupplier;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.TimeDuration;
import net.xdob.ratly.util.TimeoutExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

public class PendingStepDown {
  public static final Logger LOG = LoggerFactory.getLogger(PendingStepDown.class);

  class PendingRequest {
    private final TransferLeadershipRequest request;
    private final CompletableFuture<RaftClientReply> replyFuture = new CompletableFuture<>();

    PendingRequest(TransferLeadershipRequest request) {
      this.request = request;
    }

    CompletableFuture<RaftClientReply> getReplyFuture() {
      return replyFuture;
    }

    void complete(Function<TransferLeadershipRequest, RaftClientReply> newSuccessReply) {
      LOG.info("Successfully step down leader at {} for request {}", leader, request);
      replyFuture.complete(newSuccessReply.apply(request));
    }

    void timeout() {
      replyFuture.completeExceptionally(new TimeoutIOException(
          ": Failed to step down leader on " +  leader + "request " + request.getTimeoutMs() + "ms"));
    }

    @Override
    public String toString() {
      return request.toString();
    }
  }


  static class PendingRequestReference {
    private final AtomicReference<PendingRequest> ref = new AtomicReference<>();

    Optional<PendingRequest> getAndSetNull() {
      return Optional.ofNullable(ref.getAndSet(null));
    }

    PendingRequest getAndUpdate(Supplier<PendingRequest> supplier) {
      return ref.getAndUpdate(p -> p != null? p: supplier.get());
    }
  }

  private final LeaderStateImpl leader;
  private final TimeoutExecutor scheduler = TimeoutExecutor.getInstance();
  private final PendingRequestReference pending = new PendingRequestReference();

  PendingStepDown(LeaderStateImpl leaderState) {
    this.leader = leaderState;
  }

  CompletableFuture<RaftClientReply> submitAsync(TransferLeadershipRequest request) {
    Preconditions.assertNull(request.getNewLeader(), "request.getNewLeader()");
    final MemoizedSupplier<PendingRequest> supplier = JavaUtils.memoize(() -> new PendingRequest(request));
    final PendingRequest previous = pending.getAndUpdate(supplier);
    if (previous != null) {
      return previous.getReplyFuture();
    }
    leader.submitStepDownEvent(LeaderState.StepDownReason.FORCE);
    scheduler.onTimeout(TimeDuration.valueOf(request.getTimeoutMs(), TimeUnit.MILLISECONDS),
        this::timeout, LOG, () -> "Timeout check failed for step down leader request: " + request);
    return supplier.get().getReplyFuture();
  }

  void complete(Function<TransferLeadershipRequest, RaftClientReply> newSuccessReply) {
    pending.getAndSetNull().ifPresent(p -> p.complete(newSuccessReply));
  }

  void timeout() {
    pending.getAndSetNull().ifPresent(PendingRequest::timeout);
  }
}
