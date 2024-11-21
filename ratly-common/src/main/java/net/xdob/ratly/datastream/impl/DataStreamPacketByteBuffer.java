package net.xdob.ratly.datastream.impl;

import net.xdob.ratly.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import net.xdob.ratly.protocol.DataStreamPacket;
import net.xdob.ratly.protocol.ClientId;

import java.nio.ByteBuffer;

/**
 * Implements {@link DataStreamPacket} with {@link ByteBuffer}.
 */
public abstract class DataStreamPacketByteBuffer extends DataStreamPacketImpl {
  public static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocateDirect(0).asReadOnlyBuffer();

  private final ByteBuffer buffer;

  protected DataStreamPacketByteBuffer(ClientId clientId, Type type, long streamId, long streamOffset,
      ByteBuffer buffer) {
    super(clientId, type, streamId, streamOffset);
    this.buffer = buffer != null? buffer.asReadOnlyBuffer(): EMPTY_BYTE_BUFFER;
  }

  @Override
  public long getDataLength() {
    return buffer.remaining();
  }

  public ByteBuffer slice() {
    return buffer.slice();
  }
}
