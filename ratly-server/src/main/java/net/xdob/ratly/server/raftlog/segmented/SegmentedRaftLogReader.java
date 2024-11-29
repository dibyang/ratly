package net.xdob.ratly.server.raftlog.segmented;

import net.xdob.ratly.io.CorruptedFileException;
import net.xdob.ratly.metrics.Timekeeper;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.protocol.exceptions.ChecksumException;
import net.xdob.ratly.server.metrics.SegmentedRaftLogMetrics;
import net.xdob.ratly.server.raftlog.RaftLog;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import net.xdob.ratly.util.FileUtils;
import net.xdob.ratly.util.IOUtils;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.PureJavaCrc32C;
import net.xdob.ratly.util.SizeInBytes;
import net.xdob.ratly.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.zip.Checksum;

class SegmentedRaftLogReader implements Closeable {
  static final Logger LOG = LoggerFactory.getLogger(SegmentedRaftLogReader.class);
  /**
   * 跟踪当前流位置的 InputStream 包装器。
   * <p>
   * 此流还允许我们设置一个限制，在不抛出异常的情况下可以读取的字节数。
   */
  static class LimitedInputStream extends FilterInputStream {
    private long curPos = 0;
    private volatile long markPos = -1;
    private long limitPos = Long.MAX_VALUE;

    LimitedInputStream(InputStream is) {
      super(is);
    }

    private void checkLimit(long amt) throws IOException {
      long extra = (curPos + amt) - limitPos;
      if (extra > 0) {
        throw new IOException("Tried to read " + amt + " byte(s) past " +
            "the limit at offset " + limitPos);
      }
    }

    @Override
    public int read() throws IOException {
      checkLimit(1);
      int ret = super.read();
      if (ret != -1) {
        curPos++;
      }
      return ret;
    }

    @Override
    public int read(byte[] data) throws IOException {
      checkLimit(data.length);
      int ret = super.read(data);
      if (ret > 0) {
        curPos += ret;
      }
      return ret;
    }

    @Override
    public int read(byte[] data, int offset, int length) throws IOException {
      checkLimit(length);
      int ret = super.read(data, offset, length);
      if (ret > 0) {
        curPos += ret;
      }
      return ret;
    }

    public void setLimit(long limit) {
      limitPos = curPos + limit;
    }

    public void clearLimit() {
      limitPos = Long.MAX_VALUE;
    }

    @Override
    public synchronized void mark(int limit) {
      super.mark(limit);
      markPos = curPos;
    }

    @Override
    public synchronized void reset() throws IOException {
      if (markPos == -1) {
        throw new IOException("Not marked!");
      }
      super.reset();
      curPos = markPos;
      markPos = -1;
    }

    public long getPos() {
      return curPos;
    }

    @Override
    public long skip(long amt) throws IOException {
      long extra = (curPos + amt) - limitPos;
      if (extra > 0) {
        throw new IOException("Tried to skip " + extra + " bytes past " +
            "the limit at offset " + limitPos);
      }
      long ret = super.skip(amt);
      curPos += ret;
      return ret;
    }
  }

  private final File file;
  private final LimitedInputStream limiter;
  private final DataInputStream in;
  private byte[] temp = new byte[4096];
  private final Checksum checksum;
  private final SegmentedRaftLogMetrics raftLogMetrics;
  private final SizeInBytes maxOpSize;

  SegmentedRaftLogReader(File file, SizeInBytes maxOpSize, SegmentedRaftLogMetrics raftLogMetrics) throws IOException {
    this.file = file;
    this.limiter = new LimitedInputStream(new BufferedInputStream(FileUtils.newInputStream(file)));
    in = new DataInputStream(limiter);
    checksum = new PureJavaCrc32C();
    this.maxOpSize = maxOpSize;
    this.raftLogMetrics = raftLogMetrics;
  }

  /**
   * 从日志文件读取头部：
   * (1) 文件中的头部验证成功，返回 true。
   * (2) 文件中的头部部分写入，返回 false。
   * (3) 文件中的头部损坏或发生其他 {@link IOException}，抛出异常。
   */
  boolean verifyHeader() throws IOException {
    final int headerLength = SegmentedRaftLogFormat.getHeaderLength();
    final int readLength = in.read(temp, 0, headerLength);
    Preconditions.assertTrue(readLength <= headerLength);
    final int matchLength = SegmentedRaftLogFormat.matchHeader(temp, 0, readLength);
    Preconditions.assertTrue(matchLength <= readLength);

    if (readLength == headerLength && matchLength == readLength) {
      // The header is matched successfully
      return true;
    } else if (SegmentedRaftLogFormat.isTerminator(temp, matchLength, readLength - matchLength)) {
      // The header is partially written
      return false;
    }
    // The header is corrupted
    throw new CorruptedFileException(file, "Log header mismatched: expected header length="
        + SegmentedRaftLogFormat.getHeaderLength() + ", read length=" + readLength + ", match length=" + matchLength
        + ", header in file=" + StringUtils.bytes2HexString(temp, 0, readLength)
        + ", expected header=" + StringUtils.bytes2HexString(SegmentedRaftLogFormat.getHeaderBytebuffer()));
  }

  /**
   * 从输入流读取日志条目。
   * @return 从流中读取的操作，或在文件末尾返回 null。
   * @throws IOException 发生错误时抛出。
   */
  LogEntryProto readEntry() throws IOException {
    final Timekeeper timekeeper = Optional.ofNullable(raftLogMetrics)
        .map(SegmentedRaftLogMetrics::getReadEntryTimer)
        .orElse(null);
    try(AutoCloseable ignored = Timekeeper.start(timekeeper)) {
      return decodeEntry();
    } catch (EOFException eof) {
      in.reset();
      // The last entry is partially written.
      // It is okay to ignore it since this entry is never committed in this server.
      if (LOG.isWarnEnabled()) {
        LOG.warn("Ignoring the last partial written log entry in " + file + ": " + eof);
      } else if (LOG.isTraceEnabled()) {
        LOG.trace("Ignoring the last partial written log entry in " + file , eof);
      }
      return null;
    } catch (IOException e) {
      in.reset();

      throw e;
    } catch (Exception e) {
      // Raft 日志要求任何两个条目之间没有间隙。
      // 因此，如果条目损坏，应该抛出异常，而不是跳过损坏的条目。
      in.reset();
      throw new IOException("got unexpected exception " + e.getMessage(), e);
    }
  }

  /**
   * 扫描并验证日志条目。
   * @return 日志条目的索引
   */
  long scanEntry() throws IOException {
    return Optional.ofNullable(decodeEntry()).map(LogEntryProto::getIndex).orElse(RaftLog.INVALID_LOG_INDEX);
  }

  void verifyTerminator() throws IOException {
    // 日志的结尾应该包含 0x00 字节。
    // 如果包含其他字节，则日志本身可能已损坏。
    limiter.clearLimit();
    int numRead = -1, idx = 0;
    while (true) {
      try {
        numRead = in.read(temp);
        if (numRead == -1) {
          return;
        }
        for (idx = 0; idx < numRead; idx++) {
          if (!SegmentedRaftLogFormat.isTerminator(temp[idx])) {
            throw new IOException("Read extra bytes after the terminator at position "
                + (limiter.getPos() - numRead + idx) + " in " + file);
          }
        }
      } finally {
        // 在读取每组字节后，我们将标记位置重新设置到下一组字节之前的一位。
        // 类似地，如果发生错误，我们希望将标记重新设置到错误发生前的一位。
        if (numRead != -1) {
          in.reset();
          IOUtils.skipFully(in, idx);
          in.mark(temp.length + 1);
          IOUtils.skipFully(in, 1);
        }
      }
    }
  }

  /**
   * 解码日志条目的“帧”。这包括读取日志条目并验证校验和。
   * 输入流将在此函数结束时推进到操作的末尾。
   * @return 日志条目，如果遇到文件末尾（EOF）则返回 null。
   */
  private LogEntryProto decodeEntry() throws IOException {
    final int max = maxOpSize.getSizeInt();
    limiter.setLimit(max);
    in.mark(max);

    byte nextByte;
    try {
      nextByte = in.readByte();
    } catch (EOFException eof) {
      // 在操作码边界遇到 EOF 是预期的。
      return null;
    }
    // 每个日志条目以一个变长整数（var-int）开始。因此，一个有效条目的第一个字节不应为 0。
    // 所以，如果终止字节为 0，我们应该到达段的末尾。
    if (SegmentedRaftLogFormat.isTerminator(nextByte)) {
      verifyTerminator();
      return null;
    }

    // 在这里，我们验证操作的大小是否合理，并且数据与其校验和匹配，然后再尝试构建操作。
    int entryLength = CodedInputStream.readRawVarint32(nextByte, in);
    if (entryLength > max) {
      throw new IOException("Entry has size " + entryLength
          + ", but MAX_OP_SIZE = " + maxOpSize);
    }

    final int varintLength = CodedOutputStream.computeUInt32SizeNoTag(
        entryLength);
    final int totalLength = varintLength + entryLength;
    checkBufferSize(totalLength, max);
    in.reset();
    in.mark(max);
    IOUtils.readFully(in, temp, 0, totalLength);

    // verify checksum
    checksum.reset();
    checksum.update(temp, 0, totalLength);
    int expectedChecksum = in.readInt();
    int calculatedChecksum = (int) checksum.getValue();
    if (expectedChecksum != calculatedChecksum) {
      final String s = StringUtils.format("Log entry corrupted: Calculated checksum is %08X but read checksum is %08X.",
          calculatedChecksum, expectedChecksum);
      throw new ChecksumException(s, limiter.markPos);
    }

    // parse the buffer
    return LogEntryProto.parseFrom(
        CodedInputStream.newInstance(temp, varintLength, entryLength));
  }

  private void checkBufferSize(int entryLength, int max) {
    Preconditions.assertTrue(entryLength <= max);
    int length = temp.length;
    if (length < entryLength) {
      while (length < entryLength) {
        length = Math.min(length * 2, max);
      }
      temp = new byte[length];
    }
  }

  long getPos() {
    return limiter.getPos();
  }

  void skipFully(long length) throws IOException {
    limiter.clearLimit();
    IOUtils.skipFully(limiter, length);
  }

  @Override
  public void close() {
    IOUtils.cleanup(LOG, in);
  }
}
