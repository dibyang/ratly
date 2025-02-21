package net.xdob.ratly.statemachine.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import net.xdob.ratly.io.MD5Hash;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.storage.FileInfo;
import net.xdob.ratly.server.storage.RaftStorage;
import net.xdob.ratly.statemachine.SnapshotRetentionPolicy;
import net.xdob.ratly.statemachine.StateMachineStorage;
import net.xdob.ratly.util.AtomicFileOutputStream;
import net.xdob.ratly.util.FileUtils;
import net.xdob.ratly.util.MD5FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static net.xdob.ratly.util.MD5FileUtil.MD5_SUFFIX;

/**
 * 用于管理存储在多个文件中的状态机快照。
 * 该类提供了多种操作，包括获取、更新和清理快照文件。
 * 它的设计思路是将每个快照存储为多个文件，并提供清理和管理旧快照的机制。
 */
public class FileListStateMachineStorage implements StateMachineStorage {

  private static final Logger LOG = LoggerFactory.getLogger(FileListStateMachineStorage.class);
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
      Pattern.compile(SNAPSHOT_FILE_PREFIX + "\\.(\\d+)_(\\d+)_(.+(?<!\\.md5)$)" );
  /**
   * 用于匹配包含 MD5 校验的快照文件名的正则表达式
   */
  public static final Pattern SNAPSHOT_MD5_REGEX =
      Pattern.compile(SNAPSHOT_FILE_PREFIX + "\\.(\\d+)_(\\d+)_(.+)" + MD5_SUFFIX+"$");


  /**
   * 过滤器，用于在目录中查找符合 MD5 校验规则的文件。
   */
  private static final DirectoryStream.Filter<Path> SNAPSHOT_MD5_FILTER
      = entry -> Optional.ofNullable(entry.getFileName())
      .map(Path::toString)
      .map(SNAPSHOT_MD5_REGEX::matcher)
      .filter(Matcher::matches)
      .isPresent();
  /**
   * 存储状态机的目录，存储所有快照文件。
   */
  private volatile File stateMachineDir = null;
  /**
   * 使用 AtomicReference 存储最新的快照信息，确保线程安全。
   */
  private final AtomicReference<FileListSnapshotInfo> latestSnapshot = new AtomicReference<>();

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
  static List<FileListSnapshotInfo> getFileListSnapshotInfos(Path dir) throws IOException {
    final List<FileListSnapshotInfo> infos = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
      Map<TermIndex,List<FileInfo>> map = Maps.newConcurrentMap();
      for (Path path : stream) {
        final Path filename = path.getFileName();
        if (filename != null) {
          final Matcher matcher = SNAPSHOT_REGEX.matcher(filename.toString());
          if (matcher.matches()) {
            final long term = Long.parseLong(matcher.group(1));
            final long index = Long.parseLong(matcher.group(2));
            final String module = matcher.group(3);
            TermIndex termIndex = TermIndex.valueOf(term, index);
            final FileInfo fileInfo = new FileInfo(path, null, module); //No FileDigest here.
            List<FileInfo> fileInfos = map.computeIfAbsent(termIndex, k -> new ArrayList<>());
            fileInfos.add(fileInfo);
          }
        }
      }
      for (Map.Entry<TermIndex, List<FileInfo>> entry : map.entrySet()) {
        infos.add(new FileListSnapshotInfo( entry.getValue(), entry.getKey()));
      }
    }
    return infos;
  }

  public static void main(String[] args) throws IOException {
    String pp= SNAPSHOT_FILE_PREFIX + ".1_567_db_fspool_aio.zip.md5";
    Matcher matcher = SNAPSHOT_REGEX.matcher(pp);
    System.out.println("pp matches() = " + matcher.matches());

    Matcher matcher2 = SNAPSHOT_MD5_REGEX.matcher(pp);
    System.out.println("pp md5 matches() = " + matcher2.matches());
    System.out.println("matcher2.group(3) = " + matcher2.group(3));
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
    final List<FileListSnapshotInfo> allSnapshotFiles = getFileListSnapshotInfos(stateMachineDir.toPath());
    //按照 index 排序，保留最新的快照，删除旧的快照。
    if (allSnapshotFiles.size() > numSnapshotsRetained) {
      allSnapshotFiles.sort(Comparator.comparing(FileListSnapshotInfo::getIndex).reversed());
      allSnapshotFiles.subList(numSnapshotsRetained, allSnapshotFiles.size())
          .stream()
          .map(FileListSnapshotInfo::getFiles)
          .forEach(e->e.stream()
              .map(FileInfo::getPath)
              .forEach(snapshotPath -> {
                LOG.info("Deleting old snapshot at {}", snapshotPath.toAbsolutePath());
                FileUtils.deletePathQuietly(snapshotPath);
              })
          );

      // 删除没有对应快照文件的 MD5 文件。
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(stateMachineDir.toPath(),
          SNAPSHOT_MD5_FILTER)) {
        for (Path md5path : stream) {
          Path md5FileNamePath = md5path.getFileName();
          if (md5FileNamePath == null) {
            continue;
          }
          final String md5FileName = md5FileNamePath.toString();
          final File snapshotFile = new File(stateMachineDir,
              md5FileName.substring(0, md5FileName.length() - MD5_SUFFIX.length()));
          if (!snapshotFile.exists()) {
            FileUtils.deletePathQuietly(md5path);
          }
        }
      }
    }
  }



  /**
   * 生成临时快照文件名。
   */
  protected static String getTmpSnapshotFileName(String module, long term, long endIndex) {
    return getSnapshotFileName(module, term, endIndex) + AtomicFileOutputStream.TMP_EXTENSION;
  }
  /**
   * 生成损坏快照文件名。
   */
  protected static String getCorruptSnapshotFileName(String module, long term, long endIndex) {
    return getSnapshotFileName(module, term, endIndex) + CORRUPT_SNAPSHOT_FILE_SUFFIX;
  }

  /**
   * 生成对应 term 和 endIndex 的快照文件路径。
   */
  public File getSnapshotFile(String module, long term, long endIndex) {
    final File dir = Objects.requireNonNull(stateMachineDir, "stateMachineDir == null");
    return new File(dir, getSnapshotFileName(module, term, endIndex));
  }
  /**
   * 生成对应 term 和 endIndex 的临时文件路径。
   */
  protected File getTmpSnapshotFile(String module, long term, long endIndex) {
    final File dir = Objects.requireNonNull(stateMachineDir, "stateMachineDir == null");
    return new File(dir, getTmpSnapshotFileName(module, term, endIndex));
  }
  /**
   * 生成对应 term 和 endIndex 的损坏文件路径。
   */
  protected File getCorruptSnapshotFile(String module, long term, long endIndex) {
    final File dir = Objects.requireNonNull(stateMachineDir, "stateMachineDir == null");
    return new File(dir, getCorruptSnapshotFileName(module, term, endIndex));
  }

  /**
   * 查找目录下最新的快照文件，并返回对应的 SingleFileSnapshotInfo 对象。
   */
  static FileListSnapshotInfo findLatestSnapshot(Path dir) throws IOException {
    final Iterator<FileListSnapshotInfo> i = getFileListSnapshotInfos(dir).iterator();
    if (!i.hasNext()) {
      return null;
    }

    FileListSnapshotInfo latest = i.next();
    while (i.hasNext()) {
      final FileListSnapshotInfo info = i.next();
      if (info.getIndex() > latest.getIndex()) {
        latest = info;
      }
    }
    List<FileInfo> infos = new ArrayList<>();
    // read md5
    for (FileInfo file : latest.getFiles()) {
      final Path path = file.getPath();
      final MD5Hash md5 = MD5FileUtil.readStoredMd5ForFile(path.toFile());
      final FileInfo info = new FileInfo(path, md5, file.getModule());
      infos.add(info);
    }
    return new FileListSnapshotInfo(infos, latest.getTerm(), latest.getIndex());
  }

  public FileListSnapshotInfo updateLatestSnapshot(FileListSnapshotInfo info) {
    return latestSnapshot.updateAndGet(
        previous -> previous == null || info.getIndex() > previous.getIndex()? info: previous);
  }

  public static String getSnapshotFileName(String module, long term, long endIndex) {
    return SNAPSHOT_FILE_PREFIX  + "." + term + "_" + endIndex + "_" + module;
  }

  /**
   * 获取当前存储中的最新快照。如果已有缓存，则直接返回缓存中的快照信息，否则加载最新的快照。
   */
  @Override
  public FileListSnapshotInfo getLatestSnapshot() {
    final FileListSnapshotInfo s = latestSnapshot.get();
    if (s != null) {
      return s;
    }
    return loadLatestSnapshot();
  }

  /**
   * 加载最新的快照。如果目录为空或加载失败，返回 null。
   */
  public FileListSnapshotInfo loadLatestSnapshot() {
    final File dir = stateMachineDir;
    if (dir == null) {
      return null;
    }
    try {
      FileListSnapshotInfo latestSnapshot = findLatestSnapshot(dir.toPath());
      return updateLatestSnapshot(latestSnapshot);
    } catch (IOException ignored) {
      return null;
    }
  }

  @VisibleForTesting
  File getStateMachineDir() {
    return stateMachineDir;
  }
}
