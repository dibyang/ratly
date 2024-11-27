package net.xdob.ratly.server.storage;

import java.io.IOException;

/**
 * 持久化 Raft 存储元数据的接口
 * The file is updated atomically.
 */
public interface RaftStorageMetadataFile {
  /**
   * 读取持久化文件中的元数据。
   * @return the metadata persisted in this file.
   */
  RaftStorageMetadata getMetadata() throws IOException;

  /**
   * 将给定的元数据持久化到文件中。
   */
  void persist(RaftStorageMetadata newMetadata) throws IOException;
}
