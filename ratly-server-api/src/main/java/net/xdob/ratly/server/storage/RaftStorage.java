
package net.xdob.ratly.server.storage;

import net.xdob.ratly.server.config.Log.CorruptionPolicy;
import net.xdob.ratly.util.IOUtils;
import net.xdob.ratly.util.ReflectionUtils;
import net.xdob.ratly.util.SizeInBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * 表示 Raft 服务器的存储接口，主要用于提供 RAFT 日志和元数据的存储管理功能。
 * <p>
 * 主要负责 RAFT 服务器的存储操作，包括：
 *   1.初始化存储
 *   2.获取存储目录和元数据文件
 *   3.处理日志的腐败策略
 *   4.提供构建器（Builder）来配置和创建 RaftStorage 实例
 */
public interface RaftStorage extends Closeable {
  Logger LOG = LoggerFactory.getLogger(RaftStorage.class);

  /**
   * 初始化存储，可能会涉及创建存储目录、文件等。如果初始化失败，会抛出 IOException。
   */
  void initialize() throws IOException;

  /**
   * 返回存储目录（RaftStorageDirectory）。该目录通常用于存放 RAFT 日志和相关文件。
   * @return the storage directory.
   */
  RaftStorageDirectory getStorageDir();

  /**
   * 返回存储元数据的文件（RaftStorageMetadataFile），该文件通常存储 RAFT 存储的元数据信息，例如 RAFT 日志的索引、状态机的状态等。
   * @return the metadata file.
   */
  RaftStorageMetadataFile getMetadataFile();

  /**
   * 返回日志损坏策略（CorruptionPolicy）。该策略定义了在日志损坏时如何处理，如是否尝试恢复、是否丢弃损坏的数据等。
   * @return the corruption policy for raft log.
   */
  CorruptionPolicy getLogCorruptionPolicy();

  static Builder newBuilder() {
    return new Builder();
  }

  /**
   * 存储的启动选项
   */
  enum StartupOption {
    /**
     * 表示格式化存储。这通常在存储初始化时使用，清空现有数据并重新创建存储结构。
     */
    FORMAT,
    /**
     * 表示恢复存储。这通常用于从现有存储中恢复数据，处理日志异常等情况。
     */
    RECOVER
  }

  class Builder {

    private static final Method NEW_RAFT_STORAGE_METHOD = initNewRaftStorageMethod();

    private static Method initNewRaftStorageMethod() {
      final String className = RaftStorage.class.getPackage().getName() + ".StorageImplUtils";
      final Class<?>[] argClasses = {File.class, SizeInBytes.class, StartupOption.class, CorruptionPolicy.class};
      try {
        final Class<?> clazz = ReflectionUtils.getClassByName(className);
        return clazz.getMethod("newRaftStorage", argClasses);
      } catch (Exception e) {
        throw new IllegalStateException("Failed to initNewRaftStorageMethod", e);
      }
    }

    private static RaftStorage newRaftStorage(File dir, CorruptionPolicy logCorruptionPolicy,
        StartupOption option, SizeInBytes storageFreeSpaceMin) throws IOException {
      try {
        return (RaftStorage) NEW_RAFT_STORAGE_METHOD.invoke(null,
            dir, storageFreeSpaceMin, option, logCorruptionPolicy);
      } catch (IllegalAccessException e) {
        throw new IllegalStateException("Failed to build " + dir, e);
      } catch (InvocationTargetException e) {
        Throwable t = e.getTargetException();
        if (t.getCause() instanceof IOException) {
          throw IOUtils.asIOException(t.getCause());
        }
        throw IOUtils.asIOException(e.getCause());
      }
    }


    private File directory;
    private CorruptionPolicy logCorruptionPolicy;
    private StartupOption option;
    private SizeInBytes storageFreeSpaceMin;

    public Builder setDirectory(File directory) {
      this.directory = directory;
      return this;
    }

    public Builder setLogCorruptionPolicy(CorruptionPolicy logCorruptionPolicy) {
      this.logCorruptionPolicy = logCorruptionPolicy;
      return this;
    }

    public Builder setOption(StartupOption option) {
      this.option = option;
      return this;
    }

    public Builder setStorageFreeSpaceMin(SizeInBytes storageFreeSpaceMin) {
      this.storageFreeSpaceMin = storageFreeSpaceMin;
      return this;
    }

    public RaftStorage build() throws IOException {
      return newRaftStorage(directory, logCorruptionPolicy, option, storageFreeSpaceMin);
    }
  }
}
