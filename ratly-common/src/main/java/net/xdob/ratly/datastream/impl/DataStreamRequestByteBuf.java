
package net.xdob.ratly.datastream.impl;

import net.xdob.ratly.io.WriteOption;
import net.xdob.ratly.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import net.xdob.ratly.protocol.ClientId;
import net.xdob.ratly.protocol.DataStreamRequest;
import net.xdob.ratly.protocol.DataStreamRequestHeader;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implements {@link DataStreamRequest} with {@link ByteBuf}.
 * <p>
 * This class is immutable.
 */
public class DataStreamRequestByteBuf extends DataStreamPacketImpl implements DataStreamRequest {
  private final AtomicReference<ByteBuf> buf;
  private final List<WriteOption> options;

  public DataStreamRequestByteBuf(ClientId clientId, Type type, long streamId, long streamOffset,
                                  Iterable<WriteOption> options, ByteBuf buf) {
    super(clientId, type, streamId, streamOffset);
    this.buf = new AtomicReference<>(buf != null? buf.asReadOnly(): Unpooled.EMPTY_BUFFER);
    this.options = Collections.unmodifiableList(Lists.newArrayList(options));
  }

  public DataStreamRequestByteBuf(DataStreamRequestHeader header, ByteBuf buf) {
    this(header.getClientId(), header.getType(), header.getStreamId(), header.getStreamOffset(),
         header.getWriteOptionList(), buf);
  }

  ByteBuf getBuf() {
    return Optional.ofNullable(buf.get()).orElseThrow(
        () -> new IllegalStateException("buf is already released in " + this));
  }

  @Override
  public long getDataLength() {
    return getBuf().readableBytes();
  }

  public ByteBuf slice() {
    return getBuf().slice();
  }

  public void release() {
    final ByteBuf got = buf.getAndSet(null);
    if (got != null) {
      got.release();
    }
  }

  @Override
  public List<WriteOption> getWriteOptionList() {
    return options;
  }
}
