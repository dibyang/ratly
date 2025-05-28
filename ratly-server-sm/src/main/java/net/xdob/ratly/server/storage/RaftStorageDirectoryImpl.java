package net.xdob.ratly.server.storage;

import net.xdob.ratly.util.AtomicFileOutputStream;
import net.xdob.ratly.util.FileUtils;
import net.xdob.ratly.util.SizeInBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import static java.nio.file.Files.newDirectoryStream;

/**
 * 用于管理和操作 Raft 协议的存储目录。
 * 它提供了目录管理、存储一致性检查、空间检查、锁定和解锁等功能。
 */
class RaftStorageDirectoryImpl implements RaftStorageDirectory {
  static final Logger LOG = LoggerFactory.getLogger(RaftStorageDirectoryImpl.class);
  private static final String IN_USE_LOCK_NAME = "in_use.lock";
  private static final String META_FILE_NAME = "raft-meta";
  private static final String CONF_EXTENSION = ".conf";
  private static final String JVM_NAME = ManagementFactory.getRuntimeMXBean().getName();

  /**
   * root 是存储目录的根目录，存储所有 Raft 数据。
   */
  private final File root;
  /**
   * 该字段用于管理存储的独占锁，确保只有一个进程能够访问存储目录。
   * lock 使用 FileLock 对象来管理存储目录的访问权限。
   */
  private FileLock lock;
  /**
   * 用于设置 Raft 存储目录所需的最小空闲空间。
   */
  private final SizeInBytes freeSpaceMin;

  /**
   * Constructor
   * @param dir directory corresponding to the storage
   */
  RaftStorageDirectoryImpl(File dir, SizeInBytes freeSpaceMin) {
    this.root = dir;
    this.lock = null;
    this.freeSpaceMin = freeSpaceMin;
  }

  @Override
  public File getRoot() {
    return root;
  }

  /**
   * 用于清除存储目录中的内容并重新创建空目录。
   * Clear and re-create storage directory.
   * <p>
   * Removes contents of the current directory and creates an empty directory.
   * <p>
   * This does not fully format storage directory.
   * It cannot write the version file since it should be written last after
   * all other storage type dependent files are written.
   * Derived storage is responsible for setting specific storage values and
   * writing the version file to disk.
   */
  void clearDirectory() throws IOException {
    clearDirectory(getCurrentDir());
    clearDirectory(getStateMachineDir());
  }

  private static void clearDirectory(File dir) throws IOException {
    if (dir.exists()) {
      LOG.info("{} already exists.  Deleting it ...", dir);
      FileUtils.deleteFully(dir);
    }
    FileUtils.createDirectories(dir);
  }

  /**
   * @return 当前目录中的元数据文件。
   */
  File getMetaFile() {
    return new File(getCurrentDir(), META_FILE_NAME);
  }

  /**
   *
   * @return
   */
  File getMetaTmpFile() {
    return AtomicFileOutputStream.getTemporaryFile(getMetaFile());
  }

  File getMetaConfFile() {
    return new File(getCurrentDir(), META_FILE_NAME + CONF_EXTENSION);
  }

  /**
   * 检查 current/ 目录是否为空。如果目录不存在，认为可以进行格式化。
   */
  boolean isCurrentEmpty() throws IOException {
    File currentDir = getCurrentDir();
    if(!currentDir.exists()) {
      // if current/ does not exist, it's safe to format it.
      return true;
    }
    try(DirectoryStream<Path> dirStream =
            newDirectoryStream(currentDir.toPath())) {
      if (dirStream.iterator().hasNext()) {
        return false;
      }
    }
    return true;
  }

  /**
   * 分析存储目录的状态。检查存储目录是否存在、是否可写、是否有足够的空间以及目录是否有效。
   *
   * @return state {@link StorageState} of the storage directory
   */
  StorageState analyzeStorage(boolean toLock) throws IOException {
    Objects.requireNonNull(root, "root directory is null");

    String rootPath = root.getCanonicalPath();
    try { // check that storage exists
      if (!root.exists()) {
        LOG.info("The storage directory {} does not exist. Creating ...", rootPath);
        FileUtils.createDirectories(root);
      }
      // or is inaccessible
      if (!root.isDirectory()) {
        LOG.warn("{} is not a directory", rootPath);
        return StorageState.NON_EXISTENT;
      }
      if (!Files.isWritable(root.toPath())) {
        LOG.warn("The storage directory {} is not writable.", rootPath);
        return StorageState.NON_EXISTENT;
      }
    } catch(SecurityException ex) {
      LOG.warn("Cannot access storage directory " + rootPath, ex);
      return StorageState.NON_EXISTENT;
    }

    if (toLock) {
      this.lock(); // lock storage if it exists
    }

    // check enough space
    final long freeSpace = root.getFreeSpace();
    if (freeSpace < freeSpaceMin.getSize()) {
      LOG.warn("{} in directory {}: free space = {} < required = {}",
          StorageState.NO_SPACE, rootPath, freeSpace, freeSpaceMin);
      return StorageState.NO_SPACE;
    }

    // check whether current directory is valid
    if (isHealthy()) {
      return StorageState.NORMAL;
    } else {
      return StorageState.NOT_FORMATTED;
    }
  }

  @Override
  public boolean isHealthy() {
    return getMetaFile().exists();
  }


  @Override
  public boolean isLocked() {
    if(lock!=null){
      if(lock.isValid()){
        return true;
      }else{
        try {
          unlock();
        } catch (IOException e) {
					LOG.warn("Failed to unlock {}", this.getRoot(), e);
        }
			}
    }else{
      try {
        this.lock();
      }  catch (IOException e) {
				LOG.warn("Failed to lock {}", this.getRoot(), e);
      }
    }
    return lock!=null;
  }

  /**
   * Lock storage to provide exclusive access.
   *
   * <p> Locking is not supported by all file systems.
   * E.g., NFS does not consistently support exclusive locks.
   *
   * <p> If locking is supported we guarantee exclusive access to the
   * storage directory. Otherwise, no guarantee is given.
   *
   * @throws IOException if locking fails
   */
  void lock() throws IOException {
    final File lockF = new File(root, IN_USE_LOCK_NAME);
    final FileLock newLock = FileUtils.attempt(() -> tryLock(lockF), () -> "tryLock " + lockF);
    if (newLock == null) {
      String msg = "Cannot lock storage " + this.root
          + ". The directory is already locked";
      LOG.info(msg);
      throw new IOException(msg);
    }
    // Don't overwrite lock until success - this way if we accidentally
    // call lock twice, the internal state won't be cleared by the second
    // (failed) lock attempt
    lock = newLock;
  }

  /**
   * Attempts to acquire an exclusive lock on the storage.
   *
   * @return A lock object representing the newly-acquired lock or
   * <code>null</code> if storage is already locked.
   * @throws IOException if locking fails.
   */
  @SuppressWarnings({"squid:S2095"}) // Suppress closeable  warning
  private FileLock tryLock(File lockF) throws IOException {
//    boolean deletionHookAdded = false;
//    if (!lockF.exists()) {
//      lockF.deleteOnExit();
//      deletionHookAdded = true;
//    }
    RandomAccessFile file = new RandomAccessFile(lockF, "rws");
    FileLock res;
    try {
      res = file.getChannel().tryLock();
      if (null == res) {
        LOG.error("Unable to acquire file lock on path {}", lockF);
        throw new OverlappingFileLockException();
      }
      file.write(JVM_NAME.getBytes(StandardCharsets.UTF_8));
      LOG.info("Lock on {} acquired by nodename {}", lockF, JVM_NAME);
    } catch (OverlappingFileLockException oe) {
      // Cannot read from the locked file on Windows.
      LOG.error("It appears that another process "
          + "has already locked the storage directory: " + root, oe);
      file.close();
      throw new IOException("Failed to lock storage " + this.root + ". The directory is already locked", oe);
    } catch(IOException e) {
      LOG.error("Failed to acquire lock on " + lockF
          + ". If this storage directory is mounted via NFS, "
          + "ensure that the appropriate nfs lock services are running.", e);
      file.close();
      throw e;
    }
//    if (!deletionHookAdded) {
//      // If the file existed prior to our startup, we didn't
//      // call deleteOnExit above. But since we successfully locked
//      // the dir, we can take care of cleaning it up.
//      lockF.deleteOnExit();
//    }
    return res;
  }

  /**
   * 释放存储目录的锁，确保其他进程可以访问存储目录。
   */
  void unlock() throws IOException {
    if (this.lock == null) {
      return;
    }
    this.lock.release();
    lock.channel().close();
    lock = null;
  }

  @Override
  public String toString() {
    return "Storage Directory " + this.root;
  }
}
