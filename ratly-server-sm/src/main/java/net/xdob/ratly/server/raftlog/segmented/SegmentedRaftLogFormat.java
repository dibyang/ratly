package net.xdob.ratly.server.raftlog.segmented;

import net.xdob.ratly.util.Preconditions;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public interface SegmentedRaftLogFormat {
  class Internal {
    private static final ByteBuffer HEADER;
    private static final byte TERMINATOR_BYTE = 0;

    static {
      final byte[] bytes = "RaftLog1".getBytes(StandardCharsets.UTF_8);
      final ByteBuffer header = ByteBuffer.allocateDirect(bytes.length);
      header.put(bytes).flip();
      HEADER = header.asReadOnlyBuffer();
    }
  }

  static int getHeaderLength() {
    return Internal.HEADER.remaining();
  }

  static ByteBuffer getHeaderBytebuffer() {
    return Internal.HEADER.duplicate();
  }

  static int matchHeader(byte[] bytes, int offset, int length) {
    Preconditions.assertTrue(length <= getHeaderLength());
    for(int i = 0; i < length; i++) {
      if (bytes[offset + i] != Internal.HEADER.get(i)) {
        return i;
      }
    }
    return length;
  }

  static byte getTerminator() {
    return Internal.TERMINATOR_BYTE;
  }

  static boolean isTerminator(byte b) {
    return b == Internal.TERMINATOR_BYTE;
  }

  static boolean isTerminator(byte[] bytes, int offset, int length) {
    return indexOfNonTerminator(bytes, offset, length) == -1;
  }

  /**
   * @return The index of the first non-terminator if it exists.
   *         Otherwise, return -1, i.e. all bytes are terminator.
   */
  static int indexOfNonTerminator(byte[] bytes, int offset, int length) {
    for(int i = 0; i < length; i++) {
      if (!isTerminator(bytes[offset + i])) {
        return i;
      }
    }
    return -1;
  }
}
