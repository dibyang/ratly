
package net.xdob.ratly.netty;

import net.xdob.ratly.datastream.impl.DataStreamReplyByteBuffer;
import net.xdob.ratly.datastream.impl.DataStreamRequestByteBuffer;
import net.xdob.ratly.datastream.impl.DataStreamRequestFilePositionCount;
import net.xdob.ratly.io.FilePositionCount;
import net.xdob.ratly.io.StandardWriteOption;
import net.xdob.ratly.io.WriteOption;
import net.xdob.ratly.datastream.impl.DataStreamRequestByteBuf;
import net.xdob.ratly.proto.RaftProtos.DataStreamReplyHeaderProto;
import net.xdob.ratly.proto.RaftProtos.DataStreamRequestHeaderProto;
import net.xdob.ratly.proto.RaftProtos.DataStreamPacketHeaderProto;
import net.xdob.ratly.protocol.ClientId;
import net.xdob.ratly.protocol.DataStreamPacketHeader;
import net.xdob.ratly.protocol.DataStreamReplyHeader;
import net.xdob.ratly.protocol.DataStreamRequest;
import net.xdob.ratly.protocol.DataStreamRequestHeader;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.DefaultFileRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

public interface NettyDataStreamUtils {
  Logger LOG = LoggerFactory.getLogger(NettyDataStreamUtils.class);

  static DataStreamPacketHeaderProto.Option getOption(WriteOption option) {
    if (option == StandardWriteOption.FLUSH) {
      // FLUSH is a local option which should not be included in the header.
      return null;
    } else if (option instanceof StandardWriteOption) {
      return DataStreamPacketHeaderProto.Option.forNumber(((StandardWriteOption) option).ordinal());
    }
    throw new IllegalArgumentException("Unexpected WriteOption " + option);
  }

  static ByteBuffer getDataStreamRequestHeaderProtoByteBuffer(DataStreamRequest request) {
    final DataStreamPacketHeaderProto.Builder b = DataStreamPacketHeaderProto.newBuilder()
        .setClientId(request.getClientId().toByteString())
        .setStreamId(request.getStreamId())
        .setStreamOffset(request.getStreamOffset())
        .setType(request.getType())
        .setDataLength(request.getDataLength());

    request.getWriteOptionList().stream()
        .map(NettyDataStreamUtils::getOption)
        .filter(Objects::nonNull)
        .forEach(b::addOptions);

    return DataStreamRequestHeaderProto
        .newBuilder()
        .setPacketHeader(b)
        .build()
        .toByteString()
        .asReadOnlyByteBuffer();
  }

  static ByteBuffer getDataStreamReplyHeaderProtoByteBuf(DataStreamReplyByteBuffer reply) {
    DataStreamPacketHeaderProto.Builder b = DataStreamPacketHeaderProto
        .newBuilder()
        .setClientId(reply.getClientId().toByteString())
        .setStreamId(reply.getStreamId())
        .setStreamOffset(reply.getStreamOffset())
        .setType(reply.getType())
        .setDataLength(reply.getDataLength());
    return DataStreamReplyHeaderProto
        .newBuilder()
        .setPacketHeader(b)
        .setBytesWritten(reply.getBytesWritten())
        .setSuccess(reply.isSuccess())
        .addAllCommitInfos(reply.getCommitInfos())
        .build()
        .toByteString()
        .asReadOnlyByteBuffer();
  }

  static void encodeDataStreamRequestHeader(DataStreamRequest request, Consumer<Object> out,
      ByteBufAllocator allocator) {
    final ByteBuffer headerBuf = getDataStreamRequestHeaderProtoByteBuffer(request);

    final ByteBuf headerBodyLenBuf = allocator.directBuffer(DataStreamPacketHeader.getSizeOfHeaderBodyLen());
    headerBodyLenBuf.writeLong(headerBuf.remaining() + request.getDataLength());
    out.accept(headerBodyLenBuf);

    final ByteBuf headerLenBuf = allocator.directBuffer(DataStreamPacketHeader.getSizeOfHeaderLen());
    headerLenBuf.writeInt(headerBuf.remaining());
    out.accept(headerLenBuf);

    out.accept(Unpooled.wrappedBuffer(headerBuf));
  }

  static void encodeDataStreamRequestByteBuffer(DataStreamRequestByteBuffer request, Consumer<Object> out,
      ByteBufAllocator allocator) {
    encodeDataStreamRequestHeader(request, out, allocator);
    encodeByteBuffer(request.slice(), out);
  }

  static void encodeByteBuffer(ByteBuffer buffer, Consumer<Object> out) {
    if (buffer.remaining() == 0) {
      out.accept(Unpooled.EMPTY_BUFFER); // to avoid EncoderException: must produce at least one message
      return;
    }
    out.accept(Unpooled.wrappedBuffer(buffer));
  }

  static void encodeDataStreamRequestByteBuf(DataStreamRequestByteBuf request, Consumer<Object> out,
      ByteBufAllocator allocator) {
    encodeDataStreamRequestHeader(request, out, allocator);
    encodeByteBuf(request.slice(), out);
  }

  static void encodeByteBuf(ByteBuf buffer, Consumer<Object> out) {
    if (buffer.readableBytes() == 0) {
      out.accept(Unpooled.EMPTY_BUFFER); // to avoid EncoderException: must produce at least one message
      return;
    }
    out.accept(buffer);
  }

  static void encodeDataStreamRequestFilePositionCount(
      DataStreamRequestFilePositionCount request, Consumer<Object> out, ByteBufAllocator allocator) {
    encodeDataStreamRequestHeader(request, out, allocator);
    final FilePositionCount f = request.getFile();
    out.accept(new DefaultFileRegion(f.getFile(), f.getPosition(), f.getCount()));
  }

  static void encodeDataStreamReplyByteBuffer(DataStreamReplyByteBuffer reply, Consumer<ByteBuf> out,
      ByteBufAllocator allocator) {
    ByteBuffer headerBuf = getDataStreamReplyHeaderProtoByteBuf(reply);
    final ByteBuf headerLenBuf = allocator.directBuffer(DataStreamPacketHeader.getSizeOfHeaderLen());
    headerLenBuf.writeInt(headerBuf.remaining());
    out.accept(headerLenBuf);
    out.accept(Unpooled.wrappedBuffer(headerBuf));
    out.accept(Unpooled.wrappedBuffer(reply.slice()));
  }

  static DataStreamRequestByteBuf decodeDataStreamRequestByteBuf(ByteBuf buf) {
    return Optional.ofNullable(decodeDataStreamRequestHeader(buf))
        .map(header -> checkHeader(header, buf))
        .map(header -> new DataStreamRequestByteBuf(header, decodeData(buf, header, ByteBuf::retain)))
        .orElse(null);
  }

  static DataStreamRequestHeader decodeDataStreamRequestHeader(ByteBuf buf) {
    if (DataStreamPacketHeader.getSizeOfHeaderBodyLen() > buf.readableBytes()) {
      return null;
    }

    long headerBodyBufLen = buf.readLong();
    if (headerBodyBufLen > buf.readableBytes()) {
      buf.resetReaderIndex();
      return null;
    }

    int headerBufLen = buf.readInt();
    if (headerBufLen > buf.readableBytes()) {
      buf.resetReaderIndex();
      return null;
    }

    try {
      ByteBuf headerBuf = buf.slice(buf.readerIndex(), headerBufLen);
      DataStreamRequestHeaderProto header = DataStreamRequestHeaderProto.parseFrom(headerBuf.nioBuffer());

      final DataStreamPacketHeaderProto h = header.getPacketHeader();
      if (h.getDataLength() + headerBufLen <= buf.readableBytes()) {
        buf.readerIndex(buf.readerIndex() + headerBufLen);
        WriteOption[] options = new WriteOption[h.getOptionsCount()];
        for (int i = 0; i < options.length; i++) {
          options[i] = StandardWriteOption.values()[h.getOptions(i).ordinal()];
        }

        return new DataStreamRequestHeader(ClientId.valueOf(h.getClientId()), h.getType(), h.getStreamId(),
            h.getStreamOffset(), h.getDataLength(), options);
      } else {
        buf.resetReaderIndex();
        return null;
      }
    } catch (InvalidProtocolBufferException e) {
      LOG.error("Fail to decode request header:", e);
      buf.resetReaderIndex();
      return null;
    }
  }

  static ByteBuffer copy(ByteBuf buf) {
    final byte[] bytes = new byte[buf.readableBytes()];
    buf.readBytes(bytes);
    return ByteBuffer.wrap(bytes);
  }

  static DataStreamReplyByteBuffer decodeDataStreamReplyByteBuffer(ByteBuf buf) {
    return Optional.ofNullable(decodeDataStreamReplyHeader(buf))
        .map(header -> checkHeader(header, buf))
        .map(header -> DataStreamReplyByteBuffer.newBuilder()
            .setDataStreamReplyHeader(header)
            .setBuffer(decodeData(buf, header, NettyDataStreamUtils::copy))
            .build())
        .orElse(null);
  }

  static DataStreamReplyHeader decodeDataStreamReplyHeader(ByteBuf buf) {
    if (DataStreamPacketHeader.getSizeOfHeaderLen() > buf.readableBytes()) {
      return null;
    }

    int headerBufLen = buf.readInt();
    if (headerBufLen > buf.readableBytes()) {
      buf.resetReaderIndex();
      return null;
    }

    try {
      ByteBuf headerBuf = buf.slice(buf.readerIndex(), headerBufLen);
      DataStreamReplyHeaderProto header = DataStreamReplyHeaderProto.parseFrom(headerBuf.nioBuffer());

      final DataStreamPacketHeaderProto h = header.getPacketHeader();
      if (header.getPacketHeader().getDataLength() + headerBufLen <= buf.readableBytes()) {
        buf.readerIndex(buf.readerIndex() + headerBufLen);
        return new DataStreamReplyHeader(ClientId.valueOf(h.getClientId()), h.getType(), h.getStreamId(),
            h.getStreamOffset(), h.getDataLength(), header.getBytesWritten(), header.getSuccess(),
            header.getCommitInfosList());
      } else {
        buf.resetReaderIndex();
        return null;
      }
    } catch (InvalidProtocolBufferException e) {
      LOG.error("Fail to decode reply header:", e);
      buf.resetReaderIndex();
      return null;
    }
  }

  static <HEADER extends DataStreamPacketHeader> HEADER checkHeader(HEADER header, ByteBuf buf) {
    if (header == null) {
      return null;
    }
    if (buf.readableBytes() < header.getDataLength()) {
      buf.resetReaderIndex();
      return null;
    }
    return header;
  }

  static <DATA> DATA decodeData(ByteBuf buf, DataStreamPacketHeader header, Function<ByteBuf, DATA> toData) {
    final int dataLength = Math.toIntExact(header.getDataLength());
    final DATA data;
    if (dataLength > 0) {
      data = toData.apply(buf.slice(buf.readerIndex(), dataLength));
      buf.readerIndex(buf.readerIndex() + dataLength);
    } else {
      data = null;
    }
    buf.markReaderIndex();
    return data;
  }
}
