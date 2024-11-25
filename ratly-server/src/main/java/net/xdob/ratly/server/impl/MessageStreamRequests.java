
package net.xdob.ratly.server.impl;

import net.xdob.ratly.proto.raft.MessageStreamRequestTypeProto;
import net.xdob.ratly.protocol.ClientInvocationId;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftClientRequest;
import net.xdob.ratly.protocol.exceptions.StreamException;
import com.google.protobuf.ByteString;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.ReferenceCountedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

class MessageStreamRequests {
  public static final Logger LOG = LoggerFactory.getLogger(MessageStreamRequests.class);

  private static class PendingStream {
    private final ClientInvocationId key;
    private long nextId = -1;
    private ByteString bytes = ByteString.EMPTY;
    private final List<ReferenceCountedObject<RaftClientRequest>> pendingRefs = new LinkedList<>();

    PendingStream(ClientInvocationId key) {
      this.key = key;
    }

    synchronized CompletableFuture<ByteString> append(long messageId,
        ReferenceCountedObject<RaftClientRequest> requestRef) {
      if (nextId == -1) {
        nextId = messageId;
      } else if (messageId != nextId) {
        return JavaUtils.completeExceptionally(new StreamException(
            "Unexpected message id in " + key + ": messageId = " + messageId + " != nextId = " + nextId));
      }
      nextId++;
      final Message message = requestRef.retain().getMessage();
      pendingRefs.add(requestRef);
      bytes = bytes.concat(message.getContent());
      return CompletableFuture.completedFuture(bytes);
    }

    synchronized CompletableFuture<ReferenceCountedObject<RaftClientRequest>> getWriteRequest(long messageId,
        ReferenceCountedObject<RaftClientRequest> requestRef) {
      return append(messageId, requestRef)
          .thenApply(appended -> RaftClientRequest.toWriteRequest(requestRef.get(), () -> appended))
          .thenApply(request -> ReferenceCountedObject.delegateFrom(pendingRefs, request));
    }

    synchronized void clear() {
      pendingRefs.forEach(ReferenceCountedObject::release);
      pendingRefs.clear();
    }
  }

  static class StreamMap {
    private final Map<ClientInvocationId, PendingStream> map = new HashMap<>();

    synchronized PendingStream computeIfAbsent(ClientInvocationId key) {
      return map.computeIfAbsent(key, PendingStream::new);
    }

    synchronized PendingStream remove(ClientInvocationId key) {
      return map.remove(key);
    }

    synchronized void clear() {
      map.values().forEach(PendingStream::clear);
      map.clear();
    }
  }

  private final String name;
  private final StreamMap streams = new StreamMap();

  MessageStreamRequests(Object name) {
    this.name = name + "-" + JavaUtils.getClassSimpleName(getClass());
  }

  CompletableFuture<?> streamAsync(ReferenceCountedObject<RaftClientRequest> requestRef) {
    final RaftClientRequest request = requestRef.get();
    final MessageStreamRequestTypeProto stream = request.getType().getMessageStream();
    Preconditions.assertTrue(!stream.getEndOfRequest());
    final ClientInvocationId key = ClientInvocationId.valueOf(request.getClientId(), stream.getStreamId());
    final PendingStream pending = streams.computeIfAbsent(key);
    return pending.append(stream.getMessageId(), requestRef);
  }

  CompletableFuture<ReferenceCountedObject<RaftClientRequest>> streamEndOfRequestAsync(
      ReferenceCountedObject<RaftClientRequest> requestRef) {
    final RaftClientRequest request = requestRef.get();
    final MessageStreamRequestTypeProto stream = request.getType().getMessageStream();
    Preconditions.assertTrue(stream.getEndOfRequest());
    final ClientInvocationId key = ClientInvocationId.valueOf(request.getClientId(), stream.getStreamId());

    final PendingStream pending = streams.remove(key);
    if (pending == null) {
      return JavaUtils.completeExceptionally(new StreamException(name + ": " + key + " not found"));
    }
    return pending.getWriteRequest(stream.getMessageId(), requestRef);
  }

  void clear() {
    streams.clear();
  }
}
