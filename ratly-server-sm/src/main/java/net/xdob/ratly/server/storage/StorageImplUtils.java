
package net.xdob.ratly.server.storage;

import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.config.CorruptionPolicy;
import net.xdob.ratly.server.config.RaftServerConfigKeys;
import net.xdob.ratly.statemachine.StateMachineStorage;
import net.xdob.ratly.util.SizeInBytes;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static net.xdob.ratly.server.Division.LOG;

public final class StorageImplUtils {
  private static final File[] EMPTY_FILE_ARRAY = {};

  private StorageImplUtils() {
    //Never constructed
  }

  public static SnapshotManager newSnapshotManager(RaftPeerId id,
                                                   Supplier<RaftStorageDirectory> dir, StateMachineStorage smStorage) {
    return new SnapshotManager(id, dir, smStorage);
  }

  /** Create a {@link RaftStorageImpl}. */
  @SuppressWarnings("java:S2095") // return Closable
  public static RaftStorageImpl newRaftStorage(File dir, SizeInBytes freeSpaceMin,
                                               StartupOption option, CorruptionPolicy logCorruptionPolicy) {
    return new RaftStorageImpl(dir, freeSpaceMin, option, logCorruptionPolicy);
  }

  /** @return a list of existing subdirectories matching the given storage directory name from the given volumes. */
  static List<File> getExistingStorageSubs(List<File> volumes, String targetSubDir, Map<File, Integer> dirsPerVol) {
    return volumes.stream().flatMap(volume -> {
          final File[] dirs = Optional.ofNullable(volume.listFiles()).orElse(EMPTY_FILE_ARRAY);
          Optional.ofNullable(dirsPerVol).ifPresent(map -> map.put(volume, dirs.length));
          return Arrays.stream(dirs);
        }).filter(dir -> targetSubDir.equals(dir.getName()))
        .collect(Collectors.toList());
  }

  /** @return a volume with the min dirs. */
  static File chooseMin(Map<File, Integer> dirsPerVol) throws IOException {
    return dirsPerVol.entrySet().stream()
        .min(Map.Entry.comparingByValue())
        .map(Map.Entry::getKey)
        .orElseThrow(() -> new IOException("No storage directory found."));
  }

  /**
   * 根据给定的存储目录名称和配置属性，选择一个 {@link RaftStorage} 并尝试调用其 {@link RaftStorage#initialize()} 方法。
   * <p />
   * 当启动选项为 {@link StartupOption#FORMAT} 时：
   * - 如果存在多个现有目录，则抛出异常。
   * - 如果存在一个现有目录，则抛出异常。
   * - 如果没有现有目录，则尝试从配置属性中指定的目录列表中初始化一个新的目录，直到成功初始化一个目录或所有目录都失败。
   * <p />
   * 当启动选项为 {@link StartupOption#RECOVER} 时：
   * - 如果存在多个现有目录，则抛出异常。
   * - 如果存在一个现有目录，如果初始化失败，则抛出异常且不尝试新的目录。
   * - 如果没有现有目录，如果配置中仅指定了一个目录，则格式化该目录；
   *   否则，如果配置中指定了多个目录，则抛出异常。
   *
   * @param storageDirName 存储目录名称
   * @param option 启动选项
   * @param properties 配置属性
   * @return 成功初始化的存储对象。
   */
  public static RaftStorageImpl initRaftStorage(String storageDirName, StartupOption option,
      RaftProperties properties) throws IOException {
    return new Op(storageDirName, option, properties).run();
  }

  private static class Op {
    private final String storageDirName;
    private final StartupOption option;

    private final SizeInBytes freeSpaceMin;
    private final CorruptionPolicy logCorruptionPolicy;
    private final List<File> dirsInConf;

    private final List<File> existingSubs;
    private final Map<File, Integer> dirsPerVol = new HashMap<>();

    Op(String storageDirName, StartupOption option, RaftProperties properties) {
      this.storageDirName = storageDirName;
      this.option = option;

      this.freeSpaceMin = RaftServerConfigKeys.storageFreeSpaceMin(properties);
      this.logCorruptionPolicy = RaftServerConfigKeys.Log.corruptionPolicy(properties);
      this.dirsInConf = RaftServerConfigKeys.storageDir(properties);

      this.existingSubs = getExistingStorageSubs(dirsInConf, this.storageDirName, dirsPerVol);
    }

    RaftStorageImpl run() throws IOException {
      if (option == StartupOption.FORMAT) {
        return format();
      } else if (option == StartupOption.RECOVER) {
        final RaftStorageImpl recovered = recover();
        return recovered != null? recovered: format();
      } else {
        throw new IllegalArgumentException("Illegal option: " + option);
      }
    }

    @SuppressWarnings("java:S1181") // catch Throwable
    private RaftStorageImpl format() throws IOException {
      if (!existingSubs.isEmpty()) {
        throw new IOException("Failed to " + option + ": One or more existing directories found " + existingSubs
            + " for " + storageDirName);
      }

      while (!dirsPerVol.isEmpty()) {
        final File vol = chooseMin(dirsPerVol);
        final File dir = new File(vol, storageDirName);
        try {
          final RaftStorageImpl storage = newRaftStorage(dir, freeSpaceMin, StartupOption.FORMAT, logCorruptionPolicy);
          storage.initialize();
          return storage;
        } catch (Throwable e) {
          LOG.warn("Failed to initialize a new directory " + dir.getAbsolutePath(), e);
          dirsPerVol.remove(vol);
        }
      }
      throw new IOException("Failed to FORMAT a new storage dir for " + storageDirName + " from " + dirsInConf);
    }

    @SuppressWarnings("java:S1181") // catch Throwable
    private RaftStorageImpl recover() throws IOException {
      final int size = existingSubs.size();
      if (size > 1) {
        throw new IOException("Failed to " + option + ": More than one existing directories found "
            + existingSubs + " for " + storageDirName);
      } else if (size == 0) {
        if (dirsInConf.size() == 1) {
          // fallback to FORMAT
          return null;
        }
        throw new IOException("Failed to " + option + ": Storage directory not found for "
            + storageDirName + " from " + dirsInConf);
      }

      final File dir = existingSubs.get(0);
      try {
        final RaftStorageImpl storage = newRaftStorage(dir, freeSpaceMin, StartupOption.RECOVER, logCorruptionPolicy);
        storage.initialize();
        return storage;
      } catch (IOException e) {
        throw e;
      } catch (Throwable e) {
        throw new IOException("Failed to initialize the existing directory " + dir.getAbsolutePath(), e);
      }
    }
  }
}
