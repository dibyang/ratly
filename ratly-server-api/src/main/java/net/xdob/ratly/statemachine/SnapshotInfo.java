package net.xdob.ratly.statemachine;

import java.util.List;

import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.storage.FileInfo;

/**
 * 提供了与快照相关的核心信息，包括快照的 TermIndex（任期和日志索引）、快照文件的元信息，以及相关的便利方法。
 */
public interface SnapshotInfo {

  /**
   * 功能：返回快照对应的 TermIndex。
   * 含义：TermIndex 是快照对应的日志任期和索引位置，表示这个快照记录了状态机在这个日志位置的状态。
   * 用途：用于定位快照的时间点和一致性状态。
   * @return The term and index corresponding to this snapshot.
   */
  TermIndex getTermIndex();

  /**
   * 功能：返回快照对应的任期。
   * 实现方式：通过 getTermIndex() 获取 Term。
   * 意义：这是 TermIndex 的一个快捷方法，便于直接获取快照的任期。
   * @return The term corresponding to this snapshot.
   */
  default long getTerm() {
    return getTermIndex().getTerm();
  }

  /**
   * 功能：返回快照对应的日志索引。
   * 实现方式：通过 getTermIndex() 获取 Index。
   * 意义：这是 TermIndex 的一个快捷方法，便于直接获取快照的日志索引。
   * @return The index corresponding to this snapshot.
   */
  default long getIndex() {
    return getTermIndex().getIndex();
  }

  /**
   * 功能：返回快照包含的底层文件列表。
   * 含义：快照可能存储在多个文件中，每个文件都有自己的元信息（如路径、大小、校验和等）。该方法提供快照文件的详细信息。
   * 用途：
   * 恢复状态机：需要知道快照文件的位置和内容以重建状态。
   * 校验快照：可以通过文件的校验和验证快照的完整性。
   * @return a list of underlying files of this snapshot.
   */
  List<FileInfo> getFiles();

  List<FileInfo> getFiles(String module);

	default boolean validate(){
		return true;
	}
	default void invalid(){

	}
}
