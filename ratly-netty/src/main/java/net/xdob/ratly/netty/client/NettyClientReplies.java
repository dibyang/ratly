

package net.xdob.ratly.netty.client;

import net.xdob.ratly.proto.raft.DataStreamPacketHeaderProto.Type;
import net.xdob.ratly.protocol.ClientInvocationId;
import net.xdob.ratly.protocol.DataStreamPacket;
import net.xdob.ratly.protocol.DataStreamReply;
import io.netty.util.concurrent.ScheduledFuture;
import net.xdob.ratly.util.MemoizedSupplier;
import net.xdob.ratly.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

public class NettyClientReplies {
  public static final Logger LOG = LoggerFactory.getLogger(NettyClientReplies.class);

  private final ConcurrentMap<ClientInvocationId, ReplyMap> replies = new ConcurrentHashMap<>();

  ReplyMap getReplyMap(ClientInvocationId clientInvocationId) {
    final MemoizedSupplier<ReplyMap> q = MemoizedSupplier.valueOf(() -> new ReplyMap(clientInvocationId));
    return replies.computeIfAbsent(clientInvocationId, key -> q.get());
  }

  class ReplyMap {
    private final ClientInvocationId clientInvocationId;
    private final Map<RequestEntry, ReplyEntry> map = new ConcurrentHashMap<>();

    ReplyMap(ClientInvocationId clientInvocationId) {
      this.clientInvocationId = clientInvocationId;
    }

    ReplyEntry submitRequest(RequestEntry requestEntry, boolean isClose, CompletableFuture<DataStreamReply> f) {
      LOG.debug("put {} to the map for {}", requestEntry, clientInvocationId);
      // ConcurrentHashMap.computeIfAbsent javadoc: the function is applied at most once per key.
      return map.computeIfAbsent(requestEntry, r -> new ReplyEntry(isClose, f));
    }

    void receiveReply(DataStreamReply reply) {
      final RequestEntry requestEntry = new RequestEntry(reply);
      final ReplyEntry replyEntry = map.remove(requestEntry);
      LOG.debug("remove: {}; replyEntry: {}; reply: {}", requestEntry, replyEntry, reply);
      if (replyEntry == null) {
        LOG.debug("Request not found: {}", this);
        return;
      }
      replyEntry.complete(reply);
      if (!reply.isSuccess()) {
        failAll("a request failed with " + reply);
      } else if (replyEntry.isClosed()) {  // stream closed clean up reply map
        removeThisMap();
      }
    }

    private void removeThisMap() {
      final ReplyMap removed = replies.remove(clientInvocationId);
      Preconditions.assertSame(removed, this, "removed");
    }

    void completeExceptionally(Throwable e) {
      removeThisMap();
      for (ReplyEntry entry : map.values()) {
        entry.completeExceptionally(e);
      }
      map.clear();
    }

    private void failAll(String message) {
      completeExceptionally(new IllegalStateException(this + ": " + message));
    }

    void fail(RequestEntry requestEntry) {
      map.remove(requestEntry);
      failAll(requestEntry + " failed ");
    }

    @Override
    public String toString() {
      final StringBuilder builder = new StringBuilder();
      for (RequestEntry requestEntry : map.keySet()) {
        builder.append(requestEntry).append(", ");
      }
      return builder.toString();
    }
  }

  static class RequestEntry {
    private final long streamOffset;
    private final Type type;

    RequestEntry(DataStreamPacket packet) {
      this.streamOffset = packet.getStreamOffset();
      this.type = packet.getType();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final RequestEntry that = (RequestEntry) o;
      return streamOffset == that.streamOffset
          && type == that.type;
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, streamOffset);
    }

    @Override
    public String toString() {
      return "Request{" +
          "streamOffset=" + streamOffset +
          ", type=" + type +
          '}';
    }
  }

  static class ReplyEntry {
    private final boolean isClosed;
    private final CompletableFuture<DataStreamReply> replyFuture;
    private ScheduledFuture<?> timeoutFuture; // for reply timeout

    ReplyEntry(boolean isClosed, CompletableFuture<DataStreamReply> replyFuture) {
      this.isClosed = isClosed;
      this.replyFuture = replyFuture;
    }

    boolean isClosed() {
      return isClosed;
    }

    synchronized void complete(DataStreamReply reply) {
      cancel(timeoutFuture);
      replyFuture.complete(reply);
    }

    synchronized void completeExceptionally(Throwable t) {
      cancel(timeoutFuture);
      replyFuture.completeExceptionally(t);
    }

    static void cancel(ScheduledFuture<?> future) {
      if (future != null) {
        future.cancel(true);
      }
    }

    synchronized void scheduleTimeout(Supplier<ScheduledFuture<?>> scheduleMethod) {
      if (!replyFuture.isDone()) {
        timeoutFuture = scheduleMethod.get();
      }
    }
  }
}
