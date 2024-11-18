

package net.xdob.ratly.protocol;

import net.xdob.ratly.io.WriteOption;
import net.xdob.ratly.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import net.xdob.ratly.util.CollectionUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
/**
 * The header format is the same {@link DataStreamPacketHeader}
 * since there are no additional fields.
 */
public class DataStreamRequestHeader extends DataStreamPacketHeader implements DataStreamRequest {

  private final List<WriteOption> options;

  public DataStreamRequestHeader(ClientId clientId, Type type, long streamId, long streamOffset, long dataLength,
      WriteOption... options) {
    this(clientId, type, streamId, streamOffset, dataLength, Arrays.asList(options));
  }

  public DataStreamRequestHeader(ClientId clientId, Type type, long streamId, long streamOffset, long dataLength,
                                 Iterable<WriteOption> options) {
    super(clientId, type, streamId, streamOffset, dataLength);
    this.options = Collections.unmodifiableList(CollectionUtils.distinct(options));
  }

  @Override
  public List<WriteOption> getWriteOptionList() {
    return options;
  }
}