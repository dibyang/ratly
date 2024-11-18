
package net.xdob.ratly.datastream.impl;

import net.xdob.ratly.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import net.xdob.ratly.protocol.ClientId;
import net.xdob.ratly.protocol.DataStreamPacket;
import net.xdob.ratly.util.JavaUtils;

/**
 * This is an abstract implementation of {@link DataStreamPacket}.
 *
 * This class is immutable.
 */
public abstract class DataStreamPacketImpl implements DataStreamPacket {
  private final ClientId clientId;
  private final Type type;
  private final long streamId;
  private final long streamOffset;

  protected DataStreamPacketImpl(ClientId clientId, Type type, long streamId, long streamOffset) {
    this.clientId = clientId;
    this.type = type;
    this.streamId = streamId;
    this.streamOffset = streamOffset;
  }

  @Override
  public ClientId getClientId() {
    return clientId;
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public long getStreamId() {
    return streamId;
  }

  @Override
  public long getStreamOffset() {
    return streamOffset;
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass())
        + ":clientId=" + getClientId()
        + ",type=" + getType()
        + ",id=" + getStreamId()
        + ",offset=" + getStreamOffset()
        + ",length=" + getDataLength();
  }
}
