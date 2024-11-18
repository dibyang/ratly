
package net.xdob.ratly.client.impl;

import net.xdob.ratly.RaftConfigKeys;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RoutingTable;
import net.xdob.ratly.client.AsyncRpcApi;
import net.xdob.ratly.client.DataStreamClient;
import net.xdob.ratly.client.DataStreamClientRpc;
import net.xdob.ratly.client.DataStreamOutputRpc;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.datastream.impl.DataStreamPacketByteBuffer;
import net.xdob.ratly.datastream.impl.DataStreamReplyByteBuffer;
import net.xdob.ratly.io.FilePositionCount;
import net.xdob.ratly.io.StandardWriteOption;
import net.xdob.ratly.io.WriteOption;
import net.xdob.ratly.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import net.xdob.ratly.protocol.ClientId;
import net.xdob.ratly.protocol.ClientInvocationId;
import net.xdob.ratly.protocol.DataStreamReply;
import net.xdob.ratly.protocol.DataStreamRequestHeader;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftClientRequest;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.exceptions.AlreadyClosedException;
import net.xdob.ratly.rpc.CallId;
import io.netty.buffer.ByteBuf;
import net.xdob.ratly.util.IOUtils;
import com.google.protobuf.ByteString;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.MemoizedSupplier;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.SlidingWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Streaming client implementation
 * allows client to create streams and send asynchronously.
 */
public class DataStreamClientImpl implements DataStreamClient {
  public static final Logger LOG = LoggerFactory.getLogger(DataStreamClientImpl.class);

  private final RaftClient client;
  private final ClientId clientId;
  private final RaftGroupId groupId;

  private final RaftPeer dataStreamServer;
  private final DataStreamClientRpc dataStreamClientRpc;
  private final OrderedStreamAsync orderedStreamAsync;
  private final boolean skipSendForward;

  DataStreamClientImpl(ClientId clientId, RaftGroupId groupId, RaftPeer dataStreamServer,
      DataStreamClientRpc dataStreamClientRpc, RaftProperties properties) {
    this.skipSendForward = RaftConfigKeys.DataStream.skipSendForward(properties, LOG::info);
    this.client = null;
    this.clientId = clientId;
    this.groupId = groupId;
    this.dataStreamServer = dataStreamServer;
    this.dataStreamClientRpc = dataStreamClientRpc;
    this.orderedStreamAsync = new OrderedStreamAsync(dataStreamClientRpc, properties);
  }

  DataStreamClientImpl(RaftClient client, RaftPeer dataStreamServer,
      DataStreamClientRpc dataStreamClientRpc, RaftProperties properties) {
    this.skipSendForward = RaftConfigKeys.DataStream.skipSendForward(properties, LOG::info);
    this.client = client;
    this.clientId = client.getId();
    this.groupId = client.getGroupId();
    this.dataStreamServer = dataStreamServer;
    this.dataStreamClientRpc = dataStreamClientRpc;
    this.orderedStreamAsync = new OrderedStreamAsync(dataStreamClientRpc, properties);
  }

  public final class DataStreamOutputImpl implements DataStreamOutputRpc {
    private final RaftClientRequest header;
    private final CompletableFuture<DataStreamReply> headerFuture;
    private final SlidingWindow.Client<OrderedStreamAsync.DataStreamWindowRequest, DataStreamReply> slidingWindow;
    private final CompletableFuture<RaftClientReply> raftClientReplyFuture = new CompletableFuture<>();
    private CompletableFuture<DataStreamReply> closeFuture;
    private final MemoizedSupplier<WritableByteChannel> writableByteChannelSupplier
        = JavaUtils.memoize(() -> new WritableByteChannel() {
      @Override
      public int write(ByteBuffer src) throws IOException {
        final int remaining = src.remaining();
        // flush each call; otherwise the future will not be completed.
        final DataStreamReply reply = IOUtils.getFromFuture(writeAsync(src, StandardWriteOption.FLUSH),
            () -> "write(" + remaining + " bytes for " + ClientInvocationId.valueOf(header) + ")");
        return Math.toIntExact(reply.getBytesWritten());
      }

      @Override
      public boolean isOpen() {
        return !isClosed();
      }

      @Override
      public void close() throws IOException {
        if (isClosed()) {
          return;
        }
        IOUtils.getFromFuture(writeAsync(DataStreamPacketByteBuffer.EMPTY_BYTE_BUFFER, StandardWriteOption.CLOSE),
            () -> "close(" + ClientInvocationId.valueOf(header) + ")");
      }
    });

    private long streamOffset = 0;

    private DataStreamOutputImpl(RaftClientRequest request) {
      this.header = request;
      this.slidingWindow = new SlidingWindow.Client<>(ClientInvocationId.valueOf(clientId, header.getCallId()));
      final ByteBuffer buffer = ClientProtoUtils.toRaftClientRequestProtoByteBuffer(header);
      // TODO: ratly-1938: In order not to auto-flush the header, remove the FLUSH below.
      this.headerFuture = send(Type.STREAM_HEADER, buffer, buffer.remaining(),
          Collections.singleton(StandardWriteOption.FLUSH));
    }
    private CompletableFuture<DataStreamReply> send(Type type, Object data, long length,
                                                    Iterable<WriteOption> options) {
      final DataStreamRequestHeader h =
          new DataStreamRequestHeader(header.getClientId(), type, header.getCallId(), streamOffset, length, options);
      return orderedStreamAsync.sendRequest(h, data, slidingWindow);
    }

    private CompletableFuture<DataStreamReply> combineHeader(CompletableFuture<DataStreamReply> future) {
      return future.thenCombine(headerFuture, (reply, headerReply) -> headerReply.isSuccess()? reply : headerReply);
    }

    private CompletableFuture<DataStreamReply> writeAsyncImpl(Object data, long length, Iterable<WriteOption> options) {
      if (isClosed()) {
        return JavaUtils.completeExceptionally(new AlreadyClosedException(
            clientId + ": stream already closed, request=" + header));
      }
      final CompletableFuture<DataStreamReply> f = combineHeader(send(Type.STREAM_DATA, data, length, options));
      if (WriteOption.containsOption(options, StandardWriteOption.CLOSE)) {
        if (skipSendForward) {
          closeFuture = f;
        } else {
          closeFuture = client != null? f.thenCompose(this::sendForward): f;
        }
        closeFuture.thenApply(ClientProtoUtils::getRaftClientReply)
            .whenComplete(JavaUtils.asBiConsumer(raftClientReplyFuture));
      }
      streamOffset += length;
      return f;
    }

    public CompletableFuture<DataStreamReply> writeAsync(ByteBuf src, Iterable<WriteOption> options) {
      return writeAsyncImpl(src, src.readableBytes(), options);
    }

    @Override
    public CompletableFuture<DataStreamReply> writeAsync(ByteBuffer src, Iterable<WriteOption> options) {
      return writeAsyncImpl(src, src.remaining(), options);
    }

    @Override
    public CompletableFuture<DataStreamReply> writeAsync(FilePositionCount src, WriteOption... options) {
      return writeAsyncImpl(src, src.getCount(), Arrays.asList(options));
    }

    boolean isClosed() {
      return closeFuture != null;
    }

    @Override
    public CompletableFuture<DataStreamReply> closeAsync() {
      if (!isClosed()) {
        writeAsync(DataStreamPacketByteBuffer.EMPTY_BYTE_BUFFER, StandardWriteOption.CLOSE);
      }
      return Objects.requireNonNull(closeFuture, "closeFuture == null");
    }

    public RaftClientRequest getHeader() {
      return header;
    }

    @Override
    public CompletableFuture<DataStreamReply> getHeaderFuture() {
      return headerFuture;
    }

    @Override
    public CompletableFuture<RaftClientReply> getRaftClientReplyFuture() {
      return raftClientReplyFuture;
    }

    @Override
    public WritableByteChannel getWritableByteChannel() {
      return writableByteChannelSupplier.get();
    }

    private CompletableFuture<DataStreamReply> sendForward(DataStreamReply writeReply) {
      LOG.debug("sendForward {}", writeReply);
      if (!writeReply.isSuccess()) {
        return CompletableFuture.completedFuture(writeReply);
      }
      final AsyncRpcApi asyncRpc = (AsyncRpcApi) client.async();
      return asyncRpc.sendForward(header).thenApply(clientReply -> DataStreamReplyByteBuffer.newBuilder()
          .setClientId(clientId)
          .setType(writeReply.getType())
          .setStreamId(writeReply.getStreamId())
          .setStreamOffset(writeReply.getStreamOffset())
          .setBuffer(ClientProtoUtils.toRaftClientReplyProto(clientReply).toByteString().asReadOnlyByteBuffer())
          .setSuccess(clientReply.isSuccess())
          .setBytesWritten(writeReply.getBytesWritten())
          .setCommitInfos(clientReply.getCommitInfos())
          .build());
    }
  }

  @Override
  public DataStreamClientRpc getClientRpc() {
    return dataStreamClientRpc;
  }

  @Override
  public DataStreamOutputImpl stream(RaftClientRequest request) {
    return new DataStreamOutputImpl(request);
  }

  @Override
  public DataStreamOutputRpc stream(ByteBuffer headerMessage) {
    return stream(headerMessage, null);
  }

  @Override
  public DataStreamOutputRpc stream(ByteBuffer headerMessage, RoutingTable routingTable) {
    if (routingTable != null) {
      // Validate that the primary peer is equal to the primary peer passed by the RoutingTable
      Preconditions.assertTrue(dataStreamServer.getId().equals(routingTable.getPrimary()),
          () -> "Primary peer mismatched: the routing table has " + routingTable.getPrimary()
              + " but the client has " + dataStreamServer.getId());
    }
    final Message message =
        Optional.ofNullable(headerMessage).map(ByteString::copyFrom).map(Message::valueOf).orElse(null);
    RaftClientRequest request = RaftClientRequest.newBuilder()
        .setClientId(clientId)
        .setServerId(dataStreamServer.getId())
        .setGroupId(groupId)
        .setCallId(CallId.getAndIncrement())
        .setMessage(message)
        .setType(RaftClientRequest.dataStreamRequestType())
        .setRoutingTable(routingTable)
        .build();
    return new DataStreamOutputImpl(request);
  }

  @Override
  public void close() throws IOException {
    dataStreamClientRpc.close();
  }
}
