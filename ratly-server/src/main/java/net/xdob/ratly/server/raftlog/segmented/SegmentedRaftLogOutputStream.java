package net.xdob.ratly.server.raftlog.segmented;

import net.xdob.ratly.proto.raft.LogEntryProto;
import com.google.protobuf.CodedOutputStream;
import net.xdob.ratly.util.IOUtils;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.PureJavaCrc32C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class SegmentedRaftLogOutputStream implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(SegmentedRaftLogOutputStream.class);

  private static final ByteBuffer FILL;
  private static final int BUFFER_SIZE = 1024 * 1024; // 1 MB
  static {
    final ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    for (int i = 0; i < BUFFER_SIZE; i++) {
      buffer.put(SegmentedRaftLogFormat.getTerminator());
    }
    buffer.flip();
    FILL = buffer.asReadOnlyBuffer();
  }

  private final String name;
  private final BufferedWriteChannel out; // buffered FileChannel for writing
  private final PureJavaCrc32C checksum = new PureJavaCrc32C();

  private final long segmentMaxSize;
  private final long preallocatedSize;

  public SegmentedRaftLogOutputStream(File file, boolean append, long segmentMaxSize,
      long preallocatedSize, ByteBuffer byteBuffer)
      throws IOException {
    this.name = JavaUtils.getClassSimpleName(getClass()) + "(" + file.getName() + ")";
    this.segmentMaxSize = segmentMaxSize;
    this.preallocatedSize = preallocatedSize;
    this.out = BufferedWriteChannel.open(file, append, byteBuffer);

    if (!append) {
      // write header
      preallocateIfNecessary(SegmentedRaftLogFormat.getHeaderLength());
      out.writeToChannel(SegmentedRaftLogFormat.getHeaderBytebuffer());
      out.flush();
    }
  }

  /**
   * 将给定条目写入此输出流。
   * <p>
   * 格式：
   *   (1) 条目的序列化大小。
   *   (2) 条目本身。
   *   (3) 条目的 4 字节校验和。
   * <p>
   * 要写入的字节大小：
   *    (编码 n 的大小) + n + (校验和大小)，
   *    其中 n 是条目的序列化大小，校验和大小为 4。
   */
  public void write(LogEntryProto entry) throws IOException {
    final int serialized = entry.getSerializedSize();
    final int proto = CodedOutputStream.computeUInt32SizeNoTag(serialized) + serialized;
    final int total = proto + 4; // proto and 4-byte checksum
    preallocateIfNecessary(total);

    out.writeToBuffer(total, buf -> {
      final int pos = buf.position();
      final int protoEndPos= pos + proto;

      final CodedOutputStream encoder = CodedOutputStream.newInstance(buf);
      encoder.writeUInt32NoTag(serialized);
      entry.writeTo(encoder);

      // compute checksum
      final ByteBuffer duplicated = buf.duplicate();
      duplicated.position(pos).limit(protoEndPos);
      checksum.reset();
      checksum.update(duplicated);

      buf.position(protoEndPos);
      buf.putInt((int) checksum.getValue());
      Preconditions.assertSame(pos + total, buf.position(), "buf.position()");
    });
  }

  @Override
  public void close() throws IOException {
    try {
      flush();
    } finally {
      IOUtils.cleanup(LOG, out);
    }
  }

  /**
   * 刷新数据到持久化存储。
   * 收集同步指标。
   */
  public void flush() throws IOException {
    try {
      out.flush();
    } catch (IOException ioe) {
      String msg = "Failed to flush " + this;
      LOG.error(msg, ioe);
      throw new IOException(msg, ioe);
    }
  }

  CompletableFuture<Void> asyncFlush(ExecutorService executor) throws IOException {
    try {
      return out.asyncFlush(executor);
    } catch (IOException ioe) {
      String msg = "Failed to asyncFlush " + this;
      LOG.error(msg, ioe);
      throw new IOException(msg, ioe);
    }
  }

  private static long actualPreallocateSize(long outstandingData, long remainingSpace, long preallocate) {
    return outstandingData > remainingSpace? outstandingData
        : outstandingData > preallocate? outstandingData
        : Math.min(preallocate, remainingSpace);
  }

  private long preallocate(FileChannel fc, long outstanding) throws IOException {
    final long size = fc.size();
    final long actual = actualPreallocateSize(outstanding, segmentMaxSize - size, preallocatedSize);
    Preconditions.assertTrue(actual >= outstanding);
    final long pos = fc.position();
    LOG.debug("Preallocate {} bytes (pos={}, size={}) for {}", actual, pos, size, this);
    final long allocated = IOUtils.preallocate(fc, actual, FILL);
    Preconditions.assertSame(pos, fc.position(), "fc.position()");
    Preconditions.assertSame(actual, allocated, "allocated");
    Preconditions.assertSame(size + allocated, fc.size(), "fc.size()");
    return allocated;
  }

  private void preallocateIfNecessary(int size) throws IOException {
    out.preallocateIfNecessary(size, this::preallocate);
  }

  @Override
  public String toString() {
    return name;
  }
}
