package net.xdob.ratly.protocol;

import net.xdob.ratly.proto.RaftProtos.CommitInfoProto;

import java.util.Collection;

public interface DataStreamReply extends DataStreamPacket {

  boolean isSuccess();

  long getBytesWritten();

  /** @return the commit information when the reply is created. */
  Collection<CommitInfoProto> getCommitInfos();
}