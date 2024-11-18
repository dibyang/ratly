
package net.xdob.ratly.client.api;

import net.xdob.ratly.io.CloseAsync;
import net.xdob.ratly.io.FilePositionCount;
import net.xdob.ratly.io.WriteOption;
import net.xdob.ratly.protocol.DataStreamReply;
import net.xdob.ratly.protocol.RaftClientReply;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

/** An asynchronous output stream supporting zero buffer copying. */
public interface DataStreamOutput extends CloseAsync<DataStreamReply> {
  /**
   * Send out the data in the source buffer asynchronously.
   *
   * @param src the source buffer to be sent.
   * @param options - options specifying how the data was written
   * @return a future of the reply.
   */
  default CompletableFuture<DataStreamReply> writeAsync(ByteBuffer src, WriteOption... options) {
    return writeAsync(src, Arrays.asList(options));
  }

  /**
   * Send out the data in the source buffer asynchronously.
   *
   * @param src the source buffer to be sent.
   * @param options - options specifying how the data was written
   * @return a future of the reply.
   */
  CompletableFuture<DataStreamReply> writeAsync(ByteBuffer src, Iterable<WriteOption> options);


  /**
   * The same as writeAsync(src, 0, src.length(), options).
   */
  default CompletableFuture<DataStreamReply> writeAsync(File src, WriteOption... options) {
    return writeAsync(src, 0, src.length(), options);
  }

  /**
   * The same as writeAsync(FilePositionCount.valueOf(src, position, count), options).
   */
  default CompletableFuture<DataStreamReply> writeAsync(File src, long position, long count, WriteOption... options) {
    return writeAsync(FilePositionCount.valueOf(src, position, count), options);
  }

  /**
   * Send out the data in the source file asynchronously.
   *
   * @param src the source file with the starting position and the number of bytes.
   * @param options options specifying how the data was written
   * @return a future of the reply.
   */
  CompletableFuture<DataStreamReply> writeAsync(FilePositionCount src, WriteOption... options);

  /**
   * Return the future of the {@link RaftClientReply}
   * which will be received once this stream has been closed successfully.
   * Note that this method does not trigger closing this stream.
   *
   * @return the future of the {@link RaftClientReply}.
   */
  CompletableFuture<RaftClientReply> getRaftClientReplyFuture();

  /**
   * @return a {@link WritableByteChannel} view of this {@link DataStreamOutput}.
   */
  WritableByteChannel getWritableByteChannel();
}
