
package net.xdob.ratly.server.storage;

import net.xdob.ratly.io.CorruptedFileException;
import net.xdob.ratly.io.MD5Hash;
import net.xdob.ratly.proto.RaftProtos.FileChunkProto;
import net.xdob.ratly.proto.RaftProtos.InstallSnapshotRequestProto;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.util.ServerStringUtils;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.statemachine.StateMachine;
import net.xdob.ratly.statemachine.StateMachineStorage;
import net.xdob.ratly.util.FileUtils;
import net.xdob.ratly.util.MD5FileUtil;
import net.xdob.ratly.util.MemoizedSupplier;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.StringUtils;
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
 * Manage snapshots of a raft peer.
 * TODO: snapshot should be treated as compaction log thus can be merged into
 *       RaftLog. In this way we can have a unified getLastTermIndex interface.
 */
public class SnapshotManager {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotManager.class);

  private static final String CORRUPT = ".corrupt";
  private static final String TMP = ".tmp";

  private final RaftPeerId selfId;

  private final Supplier<File> snapshotDir;
  private final Supplier<File> snapshotTmpDir;
  private final Function<FileChunkProto, String> getRelativePath;
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

  @SuppressWarnings({"squid:S2095"}) // Suppress closeable  warning
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

  public void installSnapshot(InstallSnapshotRequestProto request, StateMachine stateMachine) throws IOException {
    final InstallSnapshotRequestProto.SnapshotChunkProto snapshotChunkRequest = request.getSnapshotChunk();
    final long lastIncludedIndex = snapshotChunkRequest.getTermIndex().getIndex();

    // create a unique temporary directory
    final File tmpDir =  new File(this.snapshotTmpDir.get(), "snapshot-" + snapshotChunkRequest.getRequestId());
    FileUtils.createDirectories(tmpDir);
    tmpDir.deleteOnExit();

    LOG.info("Installing snapshot:{}, to tmp dir:{}",
        ServerStringUtils.toInstallSnapshotRequestString(request), tmpDir);

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
          MD5FileUtil.saveMD5File(tmpSnapshotFile, digest);
        }
      }
    }

    if (snapshotChunkRequest.getDone()) {
      rename(tmpDir, snapshotDir.get());
    }
  }

  private static void rename(File tmpDir, File stateMachineDir) throws IOException {
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
