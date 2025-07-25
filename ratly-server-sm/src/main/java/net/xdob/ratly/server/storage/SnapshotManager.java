package net.xdob.ratly.server.storage;

import net.xdob.ratly.io.CorruptedFileException;
import net.xdob.ratly.io.MD5Hash;
import net.xdob.ratly.proto.raft.FileChunkProto;
import net.xdob.ratly.proto.raft.InstallSnapshotRequestProto;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.statemachine.StateMachine;
import net.xdob.ratly.statemachine.StateMachineStorage;
import net.xdob.ratly.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 负责管理 Raft 协议中的快照，并处理快照的安装和验证。
 * TODO: snapshot should be treated as compaction log thus can be merged into
 *       RaftLog. In this way we can have a unified getLastTermIndex interface.
 */
public class SnapshotManager {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotManager.class);

  private static final String CORRUPT = ".corrupt";
  private static final String TMP = ".tmp";
  /**
   * 表示当前节点的 ID。
   */
  private final RaftPeerId selfId;

  /**
   * 快照的存储目录
   */
  private final Supplier<File> snapshotDir;
  /**
   * 快照的临时目录
   */
  private final Supplier<File> snapshotTmpDir;
  /**
   * 一个函数，接受一个 FileChunkProto 类型的参数，并返回文件相对路径。
   * 这个路径是通过将文件的绝对路径与根目录路径进行比较得到的。
   */
  private final Function<FileChunkProto, String> getRelativePath;
  /**
   * 用于计算文件的 MD5 校验和，用来验证快照的完整性。
   */
  private MessageDigest digester;

  SnapshotManager(RaftPeerId selfId, Supplier<RaftStorageDirectory> dir, StateMachineStorage smStorage) {
    this.selfId = selfId;
    this.snapshotDir = MemoizedSupplier.valueOf(
        () -> Optional.ofNullable(smStorage.getSnapshotDir()).orElseGet(() -> dir.get().getStateMachineDir()));
    this.snapshotTmpDir = MemoizedSupplier.valueOf(
        () -> Optional.ofNullable(smStorage.getTmpDir()).orElseGet(() -> dir.get().getTmpDir()));

    final Supplier<Path> smDir = MemoizedSupplier.valueOf(() -> dir.get().getStateMachineDir().toPath());
    this.getRelativePath = c -> smDir.get().relativize(
        new File(dir.get().getRoot(), c.getFilename()).toPath()).toString();
  }

  /**
   * 该方法用于打开一个文件通道，并根据 chunk 的偏移量决定如何处理文件。
   * 如果偏移量为 0，它会创建一个新的临时文件，并将文件内容写入其中。
   * 如果偏移量不为 0，它会在现有的文件中追加内容。
   */
  private FileChannel open(FileChunkProto chunk, File tmpSnapshotFile) throws IOException {
    final FileChannel out;
    final boolean exists = tmpSnapshotFile.exists();
    if (chunk.getOffset() == 0) {
      // if offset is 0, delete any existing temp snapshot file if it has the same last index.
      if (exists) {
        FileUtils.deleteFully(tmpSnapshotFile);
      }
      // create the temp snapshot file and put padding inside
      out = FileUtils.newFileChannel(tmpSnapshotFile, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
      digester = MD5Hash.newDigester();
    } else {
      if (!exists) {
        throw new FileNotFoundException("Chunk offset is non-zero but file is not found: " + tmpSnapshotFile
            + ", chunk=" + chunk);
      }
      out = FileUtils.newFileChannel(tmpSnapshotFile, StandardOpenOption.WRITE)
          .position(chunk.getOffset());
    }
    return out;
  }

  public static String toInstallSnapshotRequestString(InstallSnapshotRequestProto request) {
    if (request == null) {
      return null;
    }
    final String s;
    switch (request.getInstallSnapshotRequestBodyCase()) {
      case SNAPSHOTCHUNK:
        final InstallSnapshotRequestProto.SnapshotChunkProto chunk = request.getSnapshotChunk();
        s = "chunk:" + chunk.getRequestId() + "," + chunk.getRequestIndex();
        break;
      case NOTIFICATION:
        final InstallSnapshotRequestProto.NotificationProto notification = request.getNotification();
        s = "notify:" + TermIndex.valueOf(notification.getFirstAvailableTermIndex());
        break;
      default:
        throw new IllegalStateException("Unexpected body case in " + request);
    }
    return ProtoUtils.toString(request.getServerRequest())
        + "-t" + request.getLeaderTerm()
        + "," + s;
  }


  /**
   * 该方法负责安装 Raft 协议的快照。它接收一个快照请求并将其中的文件块逐一写入到临时目录。具体步骤如下：
   * <p>
   * 1.检查快照的有效性：如果已有快照的 endIndex 大于等于当前请求的 lastIncludedIndex，则抛出异常。
   * 2.为每个文件块打开文件通道：对于每个文件块，使用 open() 方法打开文件通道并将数据写入。
   * 3.校验 MD5 摘要：当文件块传输完成后，计算文件的 MD5 摘要，并与请求中的预期摘要进行比较。如果不匹配，抛出 CorruptedFileException 异常并重命名文件为 .corrupt 文件。
   * 4.完成快照安装：如果所有文件块都成功写入并验证通过，最终将临时目录重命名为快照目录。
   * <p>
   * 该方法的关键点在于处理文件的分块传输，并在写入完成后进行数据的完整性校验。
   */
  public void installSnapshot(InstallSnapshotRequestProto request, StateMachine stateMachine) throws IOException {
    final InstallSnapshotRequestProto.SnapshotChunkProto snapshotChunkRequest = request.getSnapshotChunk();
    final long lastIncludedIndex = snapshotChunkRequest.getTermIndex().getIndex();

    // create a unique temporary directory
    final File tmpDir =  new File(this.snapshotTmpDir.get(), "snapshot-" + snapshotChunkRequest.getRequestId());
    FileUtils.createDirectories(tmpDir);
    tmpDir.deleteOnExit();

    LOG.info("Installing snapshot:{}, to tmp dir:{}",
        toInstallSnapshotRequestString(request), tmpDir);

    // TODO: Make sure that subsequent requests for the same installSnapshot are coming in order,
    // and are not lost when whole request cycle is done. Check requestId and requestIndex here
    for (FileChunkProto chunk : snapshotChunkRequest.getFileChunksList()) {
      SnapshotInfo pi = stateMachine.getLatestSnapshot();
      if (pi != null && pi.getTermIndex().getIndex() >= lastIncludedIndex) {
        throw new IOException("There exists snapshot file "
            + pi.getFiles() + " in " + selfId
            + " with endIndex >= lastIncludedIndex " + lastIncludedIndex);
      }

      final File tmpSnapshotFile = new File(tmpDir, getRelativePath.apply(chunk));
      FileUtils.createDirectoriesDeleteExistingNonDirectory(tmpSnapshotFile.getParentFile());

      try (FileChannel out = open(chunk, tmpSnapshotFile)) {
        final ByteBuffer data = chunk.getData().asReadOnlyByteBuffer();
        digester.update(data.duplicate());

        int written = 0;
        for(; data.remaining() > 0; ) {
          written += out.write(data);
        }
        Preconditions.assertSame(chunk.getData().size(), written, "written");
      }

      // rename the temp snapshot file if this is the last chunk. also verify
      // the md5 digest and create the md5 meta-file.
      if (chunk.getDone()) {
        final MD5Hash expectedDigest =
            new MD5Hash(chunk.getFileDigest().toByteArray());
        // calculate the checksum of the snapshot file and compare it with the
        // file digest in the request
        final MD5Hash digest = new MD5Hash(digester.digest());
        if (!digest.equals(expectedDigest)) {
          LOG.warn("The snapshot md5 digest {} does not match expected {}",
              digest, expectedDigest);
          // rename the temp snapshot file to .corrupt
          String renameMessage;
          try {
            final File corruptedFile = FileUtils.move(tmpSnapshotFile, CORRUPT + StringUtils.currentDateTime());
            renameMessage = "Renamed temporary snapshot file " + tmpSnapshotFile + " to " + corruptedFile;
          } catch (IOException e) {
            renameMessage = "Tried but failed to rename temporary snapshot file " + tmpSnapshotFile
                + " to a " + CORRUPT + " file";
            LOG.warn(renameMessage, e);
            renameMessage += ": " + e;
          }
          throw new CorruptedFileException(tmpSnapshotFile,
              "MD5 mismatch for snapshot-" + lastIncludedIndex + " installation.  " + renameMessage);
        } else {
          MD5FileUtil.saveDigestFile(tmpSnapshotFile, digest);
        }
      }
    }

    if (snapshotChunkRequest.getDone()) {
      rename(tmpDir, snapshotDir.get());
    }
  }

   /**
    * 该方法负责将临时目录重命名为正式的状态机目录。具体步骤如下：
    * <p>
    *    1.重命名现有状态机目录：如果状态机目录已存在，先将其重命名为 .tmp 后缀的目录。
    *    2.重命名临时目录：将安装的快照临时目录重命名为状态机目录。
    *    3.删除旧的目录：如果现有目录已被重命名，则尝试删除它。
    * <p>
    * 异常处理
    * <p>
    *    1.MD5 校验失败：如果快照文件的 MD5 校验失败，会将临时文件重命名为 .corrupt 后缀，并抛出 CorruptedFileException 异常。
    *    2.文件缺失：如果在写入文件块时遇到文件缺失的情况，将抛出 FileNotFoundException。
    *    3.写入失败：在文件写入过程中，如果发生错误，将抛出 IOException。
   */
  private void rename(File tmpDir, File stateMachineDir) throws IOException {
    LOG.info("Installed snapshot, renaming temporary dir {} to {}", tmpDir, stateMachineDir);

    // rename stateMachineDir to tmp, if it exists.
    final File existingDir;
    if (stateMachineDir.exists()) {
      File moved = null;
      try {
        moved = FileUtils.move(stateMachineDir, TMP + StringUtils.currentDateTime());
      } catch(IOException e) {
        LOG.warn("Failed to rename state machine directory " + stateMachineDir.getAbsolutePath()
            + " to a " + TMP + " directory.  Try deleting it directly.", e);
        FileUtils.deleteFully(stateMachineDir);
      }
      existingDir = moved;
    } else {
      existingDir = null;
    }

    // rename tmpDir to stateMachineDir
    try {
      FileUtils.move(tmpDir, stateMachineDir);
    } catch (IOException e) {
      throw new IOException("Failed to rename temporary director " + tmpDir.getAbsolutePath()
          + " to " + stateMachineDir.getAbsolutePath(), e);
    }

    // delete existing dir
    if (existingDir != null) {
      try {
        FileUtils.deleteFully(existingDir);
      } catch (IOException e) {
        LOG.warn("Failed to delete existing directory " + existingDir.getAbsolutePath(), e);
      }
    }
  }
}
