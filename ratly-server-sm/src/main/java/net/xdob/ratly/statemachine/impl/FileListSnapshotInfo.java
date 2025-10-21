package net.xdob.ratly.statemachine.impl;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import net.xdob.ratly.io.Digest;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.storage.FileInfo;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.MD5FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 一个不可变的类，用于表示 Raft 状态机的快照信息。
 * <p>
 * 每个快照包含一个日志条目的 Term 和 Index 以及与该快照相关的文件列表。
 * 该类实现了 SnapshotInfo 接口，提供了获取 Term 和文件列表等功能。
 */
public class FileListSnapshotInfo implements SnapshotInfo {
	static final Logger LOG = LoggerFactory.getLogger(FileListSnapshotInfo.class);
  public static final String SUM = "sum";
  private final TermIndex termIndex;
  private final List<FileInfo> files;
	private volatile long lastValidated;

  public FileListSnapshotInfo(List<FileInfo> files, TermIndex termIndex) {
    this.termIndex = termIndex;
    this.files = Collections.unmodifiableList(new ArrayList<>(files));
  }

  public FileListSnapshotInfo(List<FileInfo> files, long term, long index) {
    this(files, TermIndex.valueOf(term, index));
  }

	long getValidatedOffset() {
		return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastValidated);
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
  public List<FileInfo> getFiles(String module) {
    return files.stream().filter(e-> Objects.equals(module,e.getModule()))
        .collect(Collectors.toList());
  }

  public Optional<FileInfo> getSumFile(){
    return getFiles(SUM).stream().findFirst();
  }

	void updateValidated() {
		this.lastValidated = System.nanoTime();
	}

	@Override
	public void invalid() {
		this.lastValidated = 0;
	}

	@Override
	public boolean validate() {
		if(getValidatedOffset()<=60_000){
			return true;
		}
		for (FileInfo file : getFiles()) {
			try {
				final Digest digest = MD5FileUtil.computeDigestForFile(file.getPath().toFile());
				file.setFileDigest(digest);
				if(!digest.equals(file.getFileDigest())){
					LOG.info("Digest mismatch for file {}", file.getPath());
					return false;
				}
			} catch (IOException e) {
				LOG.info("Digest valid failed for file {}", file.getPath(), e);
				return false;
			}
		}
		updateValidated();
		return true;
	}

	@Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass()) + getTermIndex() + ":" + files;
  }
}
