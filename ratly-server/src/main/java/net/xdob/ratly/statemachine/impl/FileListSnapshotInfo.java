package net.xdob.ratly.statemachine.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.storage.FileInfo;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.util.JavaUtils;

/**
 * 一个不可变的类，用于表示 Raft 状态机的快照信息。
 * <p>
 * 每个快照包含一个日志条目的 Term 和 Index 以及与该快照相关的文件列表。
 * 该类实现了 SnapshotInfo 接口，提供了获取 Term 和文件列表等功能。
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
