
package net.xdob.ratly.client.impl;

import net.xdob.ratly.client.RaftClientConfigKeys;
import net.xdob.ratly.client.api.MessageOutputStream;
import net.xdob.ratly.client.api.MessageStreamApi;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftClientRequest;
import net.xdob.ratly.protocol.RaftClientRequest.Type;
import com.google.protobuf.ByteString;
import net.xdob.ratly.util.SizeInBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/** Send ordered asynchronous requests to a raft service. */
public final class MessageStreamImpl implements MessageStreamApi {
  public static final Logger LOG = LoggerFactory.getLogger(MessageStreamImpl.class);

  static MessageStreamImpl newInstance(RaftClientImpl client, RaftProperties properties) {
    return new MessageStreamImpl(client, properties);
  }

  class MessageOutputStreamImpl implements MessageOutputStream {
    private final long id;
    private final AtomicLong messageId = new AtomicLong();

    MessageOutputStreamImpl(long id) {
      this.id = id;
    }

    private Type getMessageStreamRequestType(boolean endOfRequest) {
      return RaftClientRequest.messageStreamRequestType(id, messageId.getAndIncrement(), endOfRequest);
    }

    @Override
    public CompletableFuture<RaftClientReply> sendAsync(Message message, boolean endOfRequest) {
      return client.async().send(getMessageStreamRequestType(endOfRequest), message, null);
    }

    @Override
    public CompletableFuture<RaftClientReply> closeAsync() {
      return client.async().send(getMessageStreamRequestType(true), null, null);
    }
  }

  private final RaftClientImpl client;
  private final SizeInBytes submessageSize;
  private final AtomicLong streamId = new AtomicLong();

  private MessageStreamImpl(RaftClientImpl client, RaftProperties properties) {
    this.client = Objects.requireNonNull(client, "client == null");
    this.submessageSize = RaftClientConfigKeys.MessageStream.submessageSize(properties);
  }

  @Override
  public MessageOutputStream stream() {
    return new MessageOutputStreamImpl(streamId.incrementAndGet());
  }

  @Override
  public CompletableFuture<RaftClientReply> streamAsync(Message message, SizeInBytes subSize) {
    final int n = subSize.getSizeInt();
    final MessageOutputStream out = stream();
    final ByteString bytes = message.getContent();
    for(int i = 0; i < bytes.size(); ) {
      final int j = Math.min(i + n, bytes.size());
      final ByteString sub = bytes.substring(i, j);
      out.sendAsync(Message.valueOf(sub));
      i = j;
    }
    return out.closeAsync();
  }

  @Override
  public CompletableFuture<RaftClientReply> streamAsync(Message message) {
    return streamAsync(message, submessageSize);
  }
}
