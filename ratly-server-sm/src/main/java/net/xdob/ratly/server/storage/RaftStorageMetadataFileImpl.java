package net.xdob.ratly.server.storage;

import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.util.AtomicFileOutputStream;
import net.xdob.ratly.util.Concurrents3;
import net.xdob.ratly.util.FileUtils;
import net.xdob.ratly.util.JavaUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 用于持久化存储 Raft协议元数据的实现类。
 * 该类通过原子操作将 Raft 存储的元数据写入磁盘上的文件，并支持加载、更新
 * 和存储 Raft 协议的关键元数据（例如当前任期号 term 和投票信息 votedFor）。
 */
class RaftStorageMetadataFileImpl implements RaftStorageMetadataFile {
  private static final String TERM_KEY = "term";
  private static final String VOTED_FOR_KEY = "votedFor";

  /**
   * 该字段表示用于存储 Raft 存储元数据的文件。
   */
  private final File file;
  /**
   * 使用 AtomicReference<RaftStorageMetadata> 来存储和操作元数据对象。
   * 通过 Concurrents3.updateAndGet 来确保元数据更新的原子性。
   */
  private final AtomicReference<RaftStorageMetadata> metadata = new AtomicReference<>();

  RaftStorageMetadataFileImpl(File file) {
    this.file = file;
  }

  /**
   * 获取存储的元数据。如果元数据尚未加载，它会从文件中加载元数据。
   * @return 存储的元数据
   */
  @Override
  public RaftStorageMetadata getMetadata() throws IOException {
    return Concurrents3.updateAndGet(metadata, value -> value != null? value: load(file));
  }

  /**
   * 将新的元数据持久化到文件中。如果新元数据与当前存储的元数据不同，则进行原子写入操作。
   * @param newMetadata 新的元数据
   */
  @Override
  public void persist(RaftStorageMetadata newMetadata) throws IOException {
    Concurrents3.updateAndGet(metadata,
        old -> Objects.equals(old, newMetadata)? old: atomicWrite(newMetadata, file));
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(RaftStorageMetadataFile.class) + ":" + file;
  }

  /**
   * 原子地将 RaftStorageMetadata（包括 term 和 votedFor）写入指定文件。
   * 写入完成后，确保通过 fsync 将数据同步到磁盘。
   *
   * @throws IOException if the file cannot be written
   */
  static RaftStorageMetadata atomicWrite(RaftStorageMetadata metadata, File file) throws IOException {
    final Properties properties = new Properties();
    properties.setProperty(TERM_KEY, Long.toString(metadata.getTerm()));
    properties.setProperty(VOTED_FOR_KEY, metadata.getVotedFor().toString());

    try(BufferedWriter out = new BufferedWriter(
        new OutputStreamWriter(new AtomicFileOutputStream(file), StandardCharsets.UTF_8))) {
      properties.store(out, "");
    }
    return metadata;
  }

  static Object getValue(String key, Properties properties) throws IOException {
    return Optional.ofNullable(properties.getProperty(key)).orElseThrow(
        () -> new IOException("'" + key + "' not found in properties: " + properties));
  }

  static long getTerm(Properties properties) throws IOException {
    try {
      return Long.parseLong((String) getValue(TERM_KEY, properties));
    } catch (Exception e) {
      throw new IOException("Failed to parse '" + TERM_KEY + "' from properties: " + properties, e);
    }
  }

  static RaftPeerId getVotedFor(Properties properties) throws IOException {
    try {
      return RaftPeerId.valueOf((String) getValue(VOTED_FOR_KEY, properties));
    } catch (Exception e) {
      throw new IOException("Failed to parse '" + VOTED_FOR_KEY + "' from properties: " + properties, e);
    }
  }

  static RaftStorageMetadata load(File file) throws IOException {
    if (!file.exists()) {
      return RaftStorageMetadata.getDefault();
    }
    try(BufferedReader br = new BufferedReader(new InputStreamReader(
        FileUtils.newInputStream(file), StandardCharsets.UTF_8))) {
      Properties properties = new Properties();
      properties.load(br);
      return RaftStorageMetadata.valueOf(getTerm(properties), getVotedFor(properties));
    } catch (IOException e) {
      throw new IOException("Failed to load " + file, e);
    }
  }
}
