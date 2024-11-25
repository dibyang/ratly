
package net.xdob.ratly.examples.filestore;

import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.client.api.DataStreamOutput;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.proto.example.DeleteReplyProto;
import net.xdob.ratly.proto.example.DeleteRequestProto;
import net.xdob.ratly.proto.example.FileStoreRequestProto;
import net.xdob.ratly.proto.example.ReadReplyProto;
import net.xdob.ratly.proto.example.ReadRequestProto;
import net.xdob.ratly.proto.example.StreamWriteRequestProto;
import net.xdob.ratly.proto.example.WriteReplyProto;
import net.xdob.ratly.proto.example.WriteRequestHeaderProto;
import net.xdob.ratly.proto.example.WriteRequestProto;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftGroup;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RoutingTable;
import net.xdob.ratly.protocol.exceptions.StateMachineException;
import com.google.protobuf.ByteString;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.ProtoUtils;
import net.xdob.ratly.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/** A standalone server using raft with a configurable state machine. */
public class FileStoreClient implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(FileStoreClient.class);

  private final RaftClient client;

  public FileStoreClient(RaftGroup group, RaftProperties properties)
      throws IOException {
    this.client = RaftClient.newBuilder()
        .setProperties(properties)
        .setRaftGroup(group)
        .build();
  }

  public FileStoreClient(RaftGroup group, RaftProperties properties, RaftPeer primaryDataStreamServer)
      throws IOException {
    this.client = RaftClient.newBuilder()
        .setProperties(properties)
        .setRaftGroup(group)
        .setPrimaryDataStreamServer(primaryDataStreamServer)
        .build();
  }

  public FileStoreClient(RaftClient client) {
    this.client = client;
  }

  @Override
  public void close() throws IOException {
    client.close();
  }

  static ByteString send(
      Message request, CheckedFunction<Message, RaftClientReply, IOException> sendFunction)
      throws IOException {
    final RaftClientReply reply = sendFunction.apply(request);
    final StateMachineException sme = reply.getStateMachineException();
    if (sme != null) {
      throw new IOException("Failed to send request " + request, sme);
    }
    Preconditions.assertTrue(reply.isSuccess(), () -> "Failed " + request + ", reply=" + reply);
    return reply.getMessage().getContent();
  }

  static CompletableFuture<ByteString> sendAsync(
      Message request, Function<Message, CompletableFuture<RaftClientReply>> sendFunction) {
    return sendFunction.apply(request).thenApply(reply -> {
      final StateMachineException sme = reply.getStateMachineException();
      if (sme != null) {
        throw new CompletionException("Failed to send request " + request, sme);
      }
      Preconditions.assertTrue(reply.isSuccess(), () -> "Failed " + request + ", reply=" + reply);
      return reply.getMessage().getContent();
    });
  }

  private ByteString send(Message request) throws IOException {
    return send(request, client.io()::send);
  }

  private ByteString sendReadOnly(Message request) throws IOException {
    return send(request, client.io()::sendReadOnly);
  }

  private CompletableFuture<ByteString> sendAsync(Message request) {
    return sendAsync(request, client.async()::send);
  }

  private CompletableFuture<ByteString> sendReadOnlyAsync(Message request) {
    return sendAsync(request, client.async()::sendReadOnly);
  }

  public ByteString read(String path, long offset, long length) throws IOException {
    final ByteString reply = readImpl(this::sendReadOnly, path, offset, length);
    return ReadReplyProto.parseFrom(reply).getData();
  }

  public CompletableFuture<ByteString> readAsync(String path, long offset, long length) {
    return readImpl(this::sendReadOnlyAsync, path, offset, length
    ).thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
        () -> ReadReplyProto.parseFrom(reply).getData()));
  }

  private static <OUTPUT, THROWABLE extends Throwable> OUTPUT readImpl(
      CheckedFunction<Message, OUTPUT, THROWABLE> sendReadOnlyFunction,
      String path, long offset, long length) throws THROWABLE {
    final ReadRequestProto read = ReadRequestProto.newBuilder()
        .setPath(ProtoUtils.toByteString(path))
        .setOffset(offset)
        .setLength(length)
        .build();

    return sendReadOnlyFunction.apply(Message.valueOf(read));
  }

  private CompletableFuture<ByteString> sendWatchAsync(Message request) {
    return sendAsync(request, client.async()::sendReadOnlyUnordered);
  }

  /**
   * Watch the path until it is created.
   */
  public CompletableFuture<ReadReplyProto> watchAsync(String path) {
    return watchImpl(this::sendWatchAsync, path)
        .thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
            () -> ReadReplyProto.parseFrom(reply)));
  }

  private static <OUTPUT, THROWABLE extends Throwable> OUTPUT watchImpl(
      CheckedFunction<Message, OUTPUT, THROWABLE> sendWatchFunction,
      String path) throws THROWABLE {
    final ReadRequestProto watch = ReadRequestProto.newBuilder()
        .setPath(ProtoUtils.toByteString(path))
        .setIsWatch(true)
        .build();

    return sendWatchFunction.apply(Message.valueOf(watch));
  }

  public long write(String path, long offset, boolean close, ByteBuffer buffer, boolean sync)
      throws IOException {
    final int chunkSize = FileStoreCommon.getChunkSize(buffer.remaining());
    buffer.limit(chunkSize);
    final ByteString reply = writeImpl(this::send, path, offset, close, buffer, sync);
    return WriteReplyProto.parseFrom(reply).getLength();
  }

  public DataStreamOutput getStreamOutput(String path, long dataSize, RoutingTable routingTable) {
    final StreamWriteRequestProto header = StreamWriteRequestProto.newBuilder()
        .setPath(ProtoUtils.toByteString(path))
        .setLength(dataSize)
        .build();
    final FileStoreRequestProto request = FileStoreRequestProto.newBuilder().setStream(header).build();
    return client.getDataStreamApi().stream(request.toByteString().asReadOnlyByteBuffer(), routingTable);
  }

  public CompletableFuture<Long> writeAsync(String path, long offset, boolean close, ByteBuffer buffer, boolean sync) {
    return writeImpl(this::sendAsync, path, offset, close, buffer, sync
    ).thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
        () -> WriteReplyProto.parseFrom(reply).getLength()));
  }

  private static <OUTPUT, THROWABLE extends Throwable> OUTPUT writeImpl(
      CheckedFunction<Message, OUTPUT, THROWABLE> sendFunction,
      String path, long offset, boolean close, ByteBuffer data, boolean sync)
      throws THROWABLE {
    final WriteRequestHeaderProto.Builder header = WriteRequestHeaderProto.newBuilder()
        .setPath(ProtoUtils.toByteString(path))
        .setOffset(offset)
        .setLength(data.remaining())
        .setClose(close)
        .setSync(sync);

    final WriteRequestProto.Builder write = WriteRequestProto.newBuilder()
        .setHeader(header)
        .setData(ByteString.copyFrom(data));

    final FileStoreRequestProto request = FileStoreRequestProto.newBuilder().setWrite(write).build();
    return sendFunction.apply(Message.valueOf(request));
  }

  private static <OUTPUT, THROWABLE extends Throwable> OUTPUT deleteImpl(
      CheckedFunction<Message, OUTPUT, THROWABLE> sendFunction, String path)
      throws THROWABLE {
    final DeleteRequestProto.Builder delete = DeleteRequestProto.newBuilder()
        .setPath(ProtoUtils.toByteString(path));
    final FileStoreRequestProto request = FileStoreRequestProto.newBuilder().setDelete(delete).build();
    return sendFunction.apply(Message.valueOf(request));
  }

  public String delete(String path) throws IOException {
    final ByteString reply = deleteImpl(this::send, path);
    return DeleteReplyProto.parseFrom(reply).getResolvedPath().toStringUtf8();
  }

  public CompletableFuture<String> deleteAsync(String path) {
    return deleteImpl(this::sendAsync, path
    ).thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
        () -> DeleteReplyProto.parseFrom(reply).getResolvedPath().toStringUtf8()));
  }
}
