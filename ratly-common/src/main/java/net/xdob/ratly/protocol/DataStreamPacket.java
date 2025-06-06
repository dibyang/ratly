package net.xdob.ratly.protocol;

import net.xdob.ratly.proto.raft.DataStreamPacketHeaderProto.Type;

public interface DataStreamPacket {
  ClientId getClientId();

  Type getType();

  long getStreamId();

  long getStreamOffset();

  long getDataLength();
}