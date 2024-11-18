
package net.xdob.ratly.server.storage;

import net.xdob.ratly.server.RaftServerConfigKeys.Log.CorruptionPolicy;
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

/** The storage of a raft server. */
public interface RaftStorage extends Closeable {
  Logger LOG = LoggerFactory.getLogger(RaftStorage.class);

  /** Initialize the storage. */
  void initialize() throws IOException;

  /** @return the storage directory. */
  RaftStorageDirectory getStorageDir();

  /** @return the metadata file. */
  RaftStorageMetadataFile getMetadataFile();

  /** @return the corruption policy for raft log. */
  CorruptionPolicy getLogCorruptionPolicy();

  static Builder newBuilder() {
    return new Builder();
  }

  enum StartupOption {
    /** Format the storage. */
    FORMAT,
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
