package net.xdob.ratly.datastream.impl;

import net.xdob.ratly.proto.raft.CommitInfoProto;
import net.xdob.ratly.proto.raft.DataStreamPacketHeaderProto.Type;
import net.xdob.ratly.protocol.ClientId;
import net.xdob.ratly.protocol.DataStreamPacket;
import net.xdob.ratly.protocol.DataStreamReply;
import net.xdob.ratly.protocol.DataStreamReplyHeader;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;

/**
 * Implements {@link DataStreamReply} with {@link ByteBuffer}.
 *
 * This class is immutable.
 */
public final class DataStreamReplyByteBuffer extends DataStreamPacketByteBuffer implements DataStreamReply {
  public static final class Builder {
    private ClientId clientId;
    private Type type;
    private long streamId;
    private long streamOffset;
    private ByteBuffer buffer;

    private boolean success;
    private long bytesWritten;
    private Collection<CommitInfoProto> commitInfos;

    private Builder() {}

    public Builder setClientId(ClientId clientId) {
      this.clientId = clientId;
      return this;
    }

    public Builder setType(Type type) {
      this.type = type;
      return this;
    }

    public Builder setStreamId(long streamId) {
      this.streamId = streamId;
      return this;
    }

    public Builder setStreamOffset(long streamOffset) {
      this.streamOffset = streamOffset;
      return this;
    }

    public Builder setBuffer(ByteBuffer buffer) {
      this.buffer = buffer;
      return this;
    }

    public Builder setSuccess(boolean success) {
      this.success = success;
      return this;
    }

    public Builder setBytesWritten(long bytesWritten) {
      this.bytesWritten = bytesWritten;
      return this;
    }

    public Builder setCommitInfos(Collection<CommitInfoProto> commitInfos) {
      this.commitInfos = commitInfos;
      return this;
    }

    public Builder setDataStreamReplyHeader(DataStreamReplyHeader header) {
      return setDataStreamPacket(header)
          .setSuccess(header.isSuccess())
          .setBytesWritten(header.getBytesWritten())
          .setCommitInfos(header.getCommitInfos());
    }

    public Builder setDataStreamPacket(DataStreamPacket packet) {
      return setClientId(packet.getClientId())
          .setType(packet.getType())
          .setStreamId(packet.getStreamId())
          .setStreamOffset(packet.getStreamOffset());
    }

    public DataStreamReplyByteBuffer build() {
      return new DataStreamReplyByteBuffer(
          clientId, type, streamId, streamOffset, buffer, success, bytesWritten, commitInfos);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final boolean success;
  private final long bytesWritten;
  private final Collection<CommitInfoProto> commitInfos;

  @SuppressWarnings("parameternumber")
  private DataStreamReplyByteBuffer(ClientId clientId, Type type, long streamId, long streamOffset, ByteBuffer buffer,
      boolean success, long bytesWritten, Collection<CommitInfoProto> commitInfos) {
    super(clientId, type, streamId, streamOffset, buffer);

    this.success = success;
    this.bytesWritten = bytesWritten;
    this.commitInfos = commitInfos != null? commitInfos: Collections.emptyList();
  }

  @Override
  public boolean isSuccess() {
    return success;
  }

  @Override
  public long getBytesWritten() {
    return bytesWritten;
  }

  @Override
  public Collection<CommitInfoProto> getCommitInfos() {
    return commitInfos;
  }

  @Override
  public String toString() {
    return super.toString()
        + "," + (success? "SUCCESS": "FAILED")
        + ",bytesWritten=" + bytesWritten;
  }
}
