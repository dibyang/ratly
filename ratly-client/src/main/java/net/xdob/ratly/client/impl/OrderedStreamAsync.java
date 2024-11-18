
package net.xdob.ratly.client.impl;

import net.xdob.ratly.client.DataStreamClientRpc;
import net.xdob.ratly.client.RaftClientConfigKeys;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.datastream.impl.DataStreamPacketByteBuffer;
import net.xdob.ratly.datastream.impl.DataStreamRequestByteBuf;
import net.xdob.ratly.datastream.impl.DataStreamRequestByteBuffer;
import net.xdob.ratly.datastream.impl.DataStreamRequestFilePositionCount;
import net.xdob.ratly.io.FilePositionCount;
import net.xdob.ratly.protocol.DataStreamReply;
import net.xdob.ratly.protocol.DataStreamRequest;
import net.xdob.ratly.protocol.DataStreamRequestHeader;
import io.netty.buffer.ByteBuf;
import net.xdob.ratly.util.IOUtils;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.SlidingWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.function.LongFunction;

public class OrderedStreamAsync {
  public static final Logger LOG = LoggerFactory.getLogger(OrderedStreamAsync.class);

  static class DataStreamWindowRequest implements SlidingWindow.ClientSideRequest<DataStreamReply> {
    private final DataStreamRequestHeader header;
    private final Object data;
    private final long seqNum;
    private final CompletableFuture<DataStreamReply> replyFuture = new CompletableFuture<>();

    DataStreamWindowRequest(DataStreamRequestHeader header, Object data, long seqNum) {
      this.header = header;
      this.data = data;
      this.seqNum = seqNum;
    }

    DataStreamRequest getDataStreamRequest() {
      if (header.getDataLength() == 0) {
        return new DataStreamRequestByteBuffer(header, DataStreamPacketByteBuffer.EMPTY_BYTE_BUFFER);
      } else if (data instanceof ByteBuf) {
        return new DataStreamRequestByteBuf(header, (ByteBuf)data);
      } else if (data instanceof ByteBuffer) {
        return new DataStreamRequestByteBuffer(header, (ByteBuffer)data);
      } else if (data instanceof FilePositionCount) {
        return new DataStreamRequestFilePositionCount(header, (FilePositionCount)data);
      }
      throw new IllegalStateException("Unexpected " + data.getClass());
    }

    @Override
    public void setFirstRequest() {
    }

    @Override
    public long getSeqNum() {
      return seqNum;
    }

    @Override
    public void setReply(DataStreamReply dataStreamReply) {
      replyFuture.complete(dataStreamReply);
    }

    @Override
    public boolean hasReply() {
      return replyFuture.isDone();
    }

    @Override
    public void fail(Throwable e) {
      replyFuture.completeExceptionally(e);
    }

    public CompletableFuture<DataStreamReply> getReplyFuture(){
      return replyFuture;
    }

    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass()) + ":seqNum=" + seqNum + "," + header;
    }
  }

  private final DataStreamClientRpc dataStreamClientRpc;

  private final Semaphore requestSemaphore;

  OrderedStreamAsync(DataStreamClientRpc dataStreamClientRpc, RaftProperties properties){
    this.dataStreamClientRpc = dataStreamClientRpc;
    this.requestSemaphore = new Semaphore(RaftClientConfigKeys.DataStream.outstandingRequestsMax(properties));
  }

  CompletableFuture<DataStreamReply> sendRequest(DataStreamRequestHeader header, Object data,
      SlidingWindow.Client<DataStreamWindowRequest, DataStreamReply> slidingWindow) {
    try {
      requestSemaphore.acquire();
    } catch (InterruptedException e){
      Thread.currentThread().interrupt();
      return JavaUtils.completeExceptionally(IOUtils.toInterruptedIOException(
          "Interrupted when sending " + JavaUtils.getClassSimpleName(data.getClass()) + ", header= " + header, e));
    }
    LOG.debug("sendRequest {}, data={}", header, data);
    final LongFunction<DataStreamWindowRequest> constructor
        = seqNum -> new DataStreamWindowRequest(header, data, seqNum);
    return slidingWindow.submitNewRequest(constructor, r -> sendRequestToNetwork(r, slidingWindow)).
           getReplyFuture().whenComplete((r, e) -> {
             if (e != null) {
               LOG.error("Failed to send request, header=" + header, e);
             }
             requestSemaphore.release();
           });
  }

  private void sendRequestToNetwork(DataStreamWindowRequest request,
      SlidingWindow.Client<DataStreamWindowRequest, DataStreamReply> slidingWindow) {
    CompletableFuture<DataStreamReply> f = request.getReplyFuture();
    if(f.isDone()) {
      return;
    }
    if(slidingWindow.isFirst(request.getSeqNum())){
      request.setFirstRequest();
    }
    final CompletableFuture<DataStreamReply> requestFuture = dataStreamClientRpc.streamAsync(
        request.getDataStreamRequest());
    long seqNum = request.getSeqNum();

    requestFuture.thenApply(reply -> {
      slidingWindow.receiveReply(
          seqNum, reply, r -> sendRequestToNetwork(r, slidingWindow));
      return reply;
    }).thenAccept(reply -> {
      if (f.isDone()) {
        return;
      }
      f.complete(reply);
    }).exceptionally(e -> {
      f.completeExceptionally(e);
      return null;
    });
  }
}
