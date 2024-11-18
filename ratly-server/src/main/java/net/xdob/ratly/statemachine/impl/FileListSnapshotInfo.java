
package net.xdob.ratly.statemachine.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.storage.FileInfo;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.util.JavaUtils;

/**
 * Each snapshot has a list of files.
 * <p>
 * The objects of this class are immutable.
 */
public class FileListSnapshotInfo implements SnapshotInfo {
  private final TermIndex termIndex;
  private final List<FileInfo> files;

  public FileListSnapshotInfo(List<FileInfo> files, TermIndex termIndex) {
    this.termIndex = termIndex;
    this.files = Collections.unmodifiableList(new ArrayList<>(files));
  }

  public FileListSnapshotInfo(List<FileInfo> files, long term, long index) {
    this(files, TermIndex.valueOf(term, index));
  }

  @Override
  public TermIndex getTermIndex() {
    return termIndex;
  }

  @Override
  public List<FileInfo> getFiles() {
    return files;
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass()) + getTermIndex() + ":" + files;
  }
}
