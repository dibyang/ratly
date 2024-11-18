

package net.xdob.ratly.protocol;

import net.xdob.ratly.io.WriteOption;

import java.util.List;

public interface DataStreamRequest extends DataStreamPacket {
  List<WriteOption> getWriteOptionList();

  /** @deprecated use {@link #getWriteOptionList()}. */
  @Deprecated
  default WriteOption[] getWriteOptions() {
    return getWriteOptionList().toArray(WriteOption.EMPTY_ARRAY);
  }
}
