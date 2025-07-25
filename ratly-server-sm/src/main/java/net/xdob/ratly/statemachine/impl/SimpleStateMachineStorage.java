package net.xdob.ratly.statemachine.impl;

import static net.xdob.ratly.util.SHA256FileUtil.SHA256_SUFFIX;

import net.xdob.ratly.io.Digest;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.storage.FileInfo;
import net.xdob.ratly.server.storage.RaftStorage;
import net.xdob.ratly.statemachine.SnapshotRetentionPolicy;
import net.xdob.ratly.statemachine.StateMachineStorage;
import com.google.common.annotations.VisibleForTesting;
import net.xdob.ratly.util.AtomicFileOutputStream;
import net.xdob.ratly.util.FileUtils;
import net.xdob.ratly.util.SHA256FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 用于管理存储在单个文件中的状态机快照。
 * 该类提供了多种操作，包括获取、更新和清理快照文件。
 * 它的设计思路是将每个快照存储为一个单独的文件，并提供清理和管理旧快照的机制。
 */
public class SimpleStateMachineStorage implements StateMachineStorage {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleStateMachineStorage.class);
  /**
   * 快照文件的前缀名
   */
  static final String SNAPSHOT_FILE_PREFIX = "snapshot";
  /**
   * 用于标记损坏快照文件的后缀
   */
  static final String CORRUPT_SNAPSHOT_FILE_SUFFIX = ".corrupt";
  /**
   * 用于匹配快照文件名的正则表达式，包含了 term 和 index 信息。
   * snapshot.term_index
   */
  public static final Pattern SNAPSHOT_REGEX =
      Pattern.compile(SNAPSHOT_FILE_PREFIX + "\\.(\\d+)_(\\d+)");
  /**
   * 用于匹配包含 SHA256 校验的快照文件名的正则表达式
   */
  public static final Pattern SNAPSHOT_SHA256_REGEX =
      Pattern.compile(SNAPSHOT_FILE_PREFIX + "\\.(\\d+)_(\\d+)" + SHA256_SUFFIX);
  /**
   * 过滤器，用于在目录中查找符合 SHA256 校验规则的文件。
   */
  private static final DirectoryStream.Filter<Path> SNAPSHOT_SHA256_FILTER
      = entry -> Optional.ofNullable(entry.getFileName())
      .map(Path::toString)
      .map(SNAPSHOT_SHA256_REGEX::matcher)
      .filter(Matcher::matches)
      .isPresent();
  /**
   * 存储状态机的目录，存储所有快照文件。
   */
  private volatile File stateMachineDir = null;
  /**
   * 使用 AtomicReference 存储最新的快照信息，确保线程安全。
   */
  private final AtomicReference<SingleFileSnapshotInfo> latestSnapshot = new AtomicReference<>();

  /**
   * 初始化状态机存储目录，并尝试加载最新的快照。
   */
  @Override
  public void init(RaftStorage storage) throws IOException {
    this.stateMachineDir = storage.getStorageDir().getStateMachineDir();
    getLatestSnapshot();
  }

  /**
   * 暂不实现，但通常用于格式化存储（例如清空或重置存储）。
   */
  @Override
  public void format() throws IOException {
    // TODO
  }

  /**
   * 扫描指定目录下的所有文件，并根据文件名解析出符合规则的快照信息。
   */
  static List<SingleFileSnapshotInfo> getSingleFileSnapshotInfos(Path dir) throws IOException {
    final List<SingleFileSnapshotInfo> infos = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
      for (Path path : stream) {
        final Path filename = path.getFileName();
        if (filename != null) {
          final Matcher matcher = SNAPSHOT_REGEX.matcher(filename.toString());
          if (matcher.matches()) {
            final long term = Long.parseLong(matcher.group(1));
            final long index = Long.parseLong(matcher.group(2));
            final FileInfo fileInfo = new FileInfo(path, null); //No FileDigest here.
            infos.add(new SingleFileSnapshotInfo(fileInfo, term, index));
          }
        }
      }
    }
    return infos;
  }

  /**
   * 功能：根据快照保留策略清理过期的快照文件。如果快照数量超过保留的数量，它会删除旧的快照文件。
   * 步骤：
   *    1.获取所有的快照文件信息。
   *    2.按照 index 排序，保留最新的快照，删除旧的快照。
   *    3.删除没有对应快照文件的 MD5 文件。
   */
  @Override
  public void cleanupOldSnapshots(SnapshotRetentionPolicy snapshotRetentionPolicy) throws IOException {
    if (stateMachineDir == null) {
      return;
    }

    final int numSnapshotsRetained = Optional.ofNullable(snapshotRetentionPolicy)
        .map(SnapshotRetentionPolicy::getNumSnapshotsRetained)
        .orElse(SnapshotRetentionPolicy.DEFAULT_ALL_SNAPSHOTS_RETAINED);
    if (numSnapshotsRetained <= 0) {
      return;
    }
    //获取所有的快照文件信息。
    final List<SingleFileSnapshotInfo> allSnapshotFiles = getSingleFileSnapshotInfos(stateMachineDir.toPath());
    //按照 index 排序，保留最新的快照，删除旧的快照。
    if (allSnapshotFiles.size() > numSnapshotsRetained) {
      allSnapshotFiles.sort(Comparator.comparing(SingleFileSnapshotInfo::getIndex).reversed());
      allSnapshotFiles.subList(numSnapshotsRetained, allSnapshotFiles.size())
          .stream()
          .map(SingleFileSnapshotInfo::getFile)
          .map(FileInfo::getPath)
          .forEach(snapshotPath -> {
            LOG.info("Deleting old snapshot at {}", snapshotPath.toAbsolutePath());
            FileUtils.deletePathQuietly(snapshotPath);
          });
      // 删除没有对应快照文件的 MD5 文件。
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(stateMachineDir.toPath(),
          SNAPSHOT_SHA256_FILTER)) {
        for (Path md5path : stream) {
          Path md5FileNamePath = md5path.getFileName();
          if (md5FileNamePath == null) {
            continue;
          }
          final String md5FileName = md5FileNamePath.toString();
          final File snapshotFile = new File(stateMachineDir,
              md5FileName.substring(0, md5FileName.length() - SHA256_SUFFIX.length()));
          if (!snapshotFile.exists()) {
            FileUtils.deletePathQuietly(md5path);
          }
        }
      }
    }
  }

  /**
   * 根据快照文件的命名规则，解析出文件的 term 和 index，并返回对应的 TermIndex。
   */
  public static TermIndex getTermIndexFromSnapshotFile(File file) {
    final String name = file.getName();
    final Matcher m = SNAPSHOT_REGEX.matcher(name);
    if (!m.matches()) {
      throw new IllegalArgumentException("File \"" + file
          + "\" does not match snapshot file name pattern \""
          + SNAPSHOT_REGEX + "\"");
    }
    final long term = Long.parseLong(m.group(1));
    final long index = Long.parseLong(m.group(2));
    return TermIndex.valueOf(term, index);
  }

  /**
   * 生成临时快照文件名。
   */
  protected static String getTmpSnapshotFileName(long term, long endIndex) {
    return getSnapshotFileName(term, endIndex) + AtomicFileOutputStream.TMP_EXTENSION;
  }
  /**
   * 生成损坏快照文件名。
   */
  protected static String getCorruptSnapshotFileName(long term, long endIndex) {
    return getSnapshotFileName(term, endIndex) + CORRUPT_SNAPSHOT_FILE_SUFFIX;
  }

  /**
   * 生成对应 term 和 endIndex 的快照文件路径。
   */
  public File getSnapshotFile(long term, long endIndex) {
    final File dir = Objects.requireNonNull(stateMachineDir, "stateMachineDir == null");
    return new File(dir, getSnapshotFileName(term, endIndex));
  }
  /**
   * 生成对应 term 和 endIndex 的临时文件路径。
   */
  protected File getTmpSnapshotFile(long term, long endIndex) {
    final File dir = Objects.requireNonNull(stateMachineDir, "stateMachineDir == null");
    return new File(dir, getTmpSnapshotFileName(term, endIndex));
  }
  /**
   * 生成对应 term 和 endIndex 的损坏文件路径。
   */
  protected File getCorruptSnapshotFile(long term, long endIndex) {
    final File dir = Objects.requireNonNull(stateMachineDir, "stateMachineDir == null");
    return new File(dir, getCorruptSnapshotFileName(term, endIndex));
  }

  /**
   * 查找目录下最新的快照文件，并返回对应的 SingleFileSnapshotInfo 对象。
   */
  static SingleFileSnapshotInfo findLatestSnapshot(Path dir) throws IOException {
    final Iterator<SingleFileSnapshotInfo> i = getSingleFileSnapshotInfos(dir).iterator();
    if (!i.hasNext()) {
      return null;
    }

    SingleFileSnapshotInfo latest = i.next();
    while (i.hasNext()) {
      final SingleFileSnapshotInfo info = i.next();
      if (info.getIndex() > latest.getIndex()) {
        latest = info;
      }
    }

    // read digest
    final Path path = latest.getFile().getPath();
    final Digest digest = SHA256FileUtil.readStoredDigestForFile(path.toFile());
    final FileInfo info = new FileInfo(path, digest);
    return new SingleFileSnapshotInfo(info, latest.getTerm(), latest.getIndex());
  }

  public SingleFileSnapshotInfo updateLatestSnapshot(SingleFileSnapshotInfo info) {
    return latestSnapshot.updateAndGet(
        previous -> previous == null || info.getIndex() > previous.getIndex()? info: previous);
  }

  public static String getSnapshotFileName(long term, long endIndex) {
    return SNAPSHOT_FILE_PREFIX + "." + term + "_" + endIndex;
  }

  /**
   * 获取当前存储中的最新快照。如果已有缓存，则直接返回缓存中的快照信息，否则加载最新的快照。
   */
  @Override
  public SingleFileSnapshotInfo getLatestSnapshot() {
    final SingleFileSnapshotInfo s = latestSnapshot.get();
    if (s != null) {
      return s;
    }
    return loadLatestSnapshot();
  }

  /**
   * 加载最新的快照。如果目录为空或加载失败，返回 null。
   */
  public SingleFileSnapshotInfo loadLatestSnapshot() {
    final File dir = stateMachineDir;
    if (dir == null) {
      return null;
    }
    try {
      return updateLatestSnapshot(findLatestSnapshot(dir.toPath()));
    } catch (IOException ignored) {
      return null;
    }
  }

  @VisibleForTesting
  File getStateMachineDir() {
    return stateMachineDir;
  }
}
