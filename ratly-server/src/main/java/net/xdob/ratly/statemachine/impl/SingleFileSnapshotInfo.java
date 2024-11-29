package net.xdob.ratly.statemachine.impl;

import java.util.Collections;

import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.storage.FileInfo;

/**
 * 一个轻量级的类，用于表示只有一个文件的状态机快照。
 * 它的实现非常简单，通过继承 FileListSnapshotInfo 来复用大部分逻辑，
 * 并通过提供 getFile() 方法，允许轻松获取该快照关联的文件。
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
