

package net.xdob.ratly.protocol;

import net.xdob.ratly.proto.RaftProtos.CommitInfoProto;
import net.xdob.ratly.proto.RaftProtos.DataStreamPacketHeaderProto.Type;

import java.util.Collection;
import java.util.Collections;

/** The header format is {@link DataStreamPacketHeader}, bytesWritten and flags. */
public class DataStreamReplyHeader extends DataStreamPacketHeader implements DataStreamReply {
  private final long bytesWritten;
  private final boolean success;
  private final Collection<CommitInfoProto> commitInfos;

  @SuppressWarnings("parameternumber")
  public DataStreamReplyHeader(ClientId clientId, Type type, long streamId, long streamOffset, long dataLength,
      long bytesWritten, boolean success, Collection<CommitInfoProto> commitInfos) {
    super(clientId, type, streamId, streamOffset, dataLength);
    this.bytesWritten = bytesWritten;
    this.success = success;
    this.commitInfos = commitInfos != null? commitInfos: Collections.emptyList();
  }

  @Override
  public long getBytesWritten() {
    return bytesWritten;
  }

  @Override
  public boolean isSuccess() {
    return success;
  }

  @Override
  public Collection<CommitInfoProto> getCommitInfos() {
    return commitInfos;
  }
}