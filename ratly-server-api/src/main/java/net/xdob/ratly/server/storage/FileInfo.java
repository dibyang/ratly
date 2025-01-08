package net.xdob.ratly.server.storage;

import java.nio.file.Path;

import net.xdob.ratly.io.MD5Hash;

/**
 * 描述了文件的元数据，主要用于存储与文件相关的路径、MD5 校验值和文件大小等信息。
 * 它是不可变的，这意味着一旦创建后，文件的元数据就不可更改。
 */
public class FileInfo {
  private final Path path;
  private final MD5Hash fileDigest;
  private final long fileSize;
  private final String module;

  public FileInfo(Path path, MD5Hash fileDigest, String module) {
    this.path = path;
    this.fileDigest = fileDigest;
    this.fileSize = path.toFile().length();
    this.module = module;
  }

  public FileInfo(Path path, MD5Hash fileDigest) {
    this(path, fileDigest, null);
  }

  @Override
  public String toString() {
    return path.toString();
  }

  /** @return the path of the file. */
  public Path getPath() {
    return path;
  }

  /** @return the MD5 file digest of the file. */
  public MD5Hash getFileDigest() {
    return fileDigest;
  }

  /** @return the size of the file. */
  public long getFileSize() {
    return fileSize;
  }

  /**
   * 模块名
   * @return 模块名
   */
  public String getModule() {
    return module;
  }
}
