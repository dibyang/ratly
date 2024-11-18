
package net.xdob.ratly.datastream.impl;

import net.xdob.ratly.io.FilePositionCount;
import net.xdob.ratly.io.WriteOption;
import net.xdob.ratly.protocol.DataStreamRequest;
import net.xdob.ratly.protocol.DataStreamRequestHeader;

import java.util.List;

/**
 * Implements {@link DataStreamRequest} with {@link FilePositionCount}.
 * <p>
 * This class is immutable.
 */
public class DataStreamRequestFilePositionCount extends DataStreamPacketImpl implements DataStreamRequest {
  private final FilePositionCount file;
  private final List<WriteOption> options;

  public DataStreamRequestFilePositionCount(DataStreamRequestHeader header, FilePositionCount file) {
    super(header.getClientId(), header.getType(), header.getStreamId(), header.getStreamOffset());
    this.options = header.getWriteOptionList();
    this.file = file;
  }

  @Override
  public long getDataLength() {
    return file.getCount();
  }

  /** @return the file with the starting position and the byte count. */
  public FilePositionCount getFile() {
    return file;
  }

  @Override
  public List<WriteOption> getWriteOptionList() {
    return options;
  }
}
