package net.xdob.ratly.datastream.impl;

import net.xdob.ratly.io.WriteOption;
import net.xdob.ratly.protocol.DataStreamRequest;
import net.xdob.ratly.protocol.DataStreamRequestHeader;
import net.xdob.ratly.util.Preconditions;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Implements {@link DataStreamRequest} with {@link ByteBuffer}.
 * <p>
 * This class is immutable.
 */
public class DataStreamRequestByteBuffer extends DataStreamPacketByteBuffer implements DataStreamRequest {
  private final List<WriteOption> options;

  public DataStreamRequestByteBuffer(DataStreamRequestHeader header, ByteBuffer buffer) {
    super(header.getClientId(), header.getType(), header.getStreamId(), header.getStreamOffset(), buffer);
    this.options = header.getWriteOptionList();
    Preconditions.assertTrue(header.getDataLength() == buffer.remaining());
  }

  @Override
  public List<WriteOption> getWriteOptionList() {
    return options;
  }
}
