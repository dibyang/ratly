
package net.xdob.ratly.statemachine.impl;

import java.util.Collections;

import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.storage.FileInfo;

/**
 * Each snapshot only has a single file.
 * <p>
 * The objects of this class are immutable.
 */
public class SingleFileSnapshotInfo extends FileListSnapshotInfo {
  public SingleFileSnapshotInfo(FileInfo fileInfo, TermIndex termIndex) {
    super(Collections.singletonList(fileInfo), termIndex);
  }

  public SingleFileSnapshotInfo(FileInfo fileInfo, long term, long endIndex) {
    this(fileInfo, TermIndex.valueOf(term, endIndex));
  }

  /** @return the file associated with the snapshot. */
  public FileInfo getFile() {
    return getFiles().get(0);
  }
}
