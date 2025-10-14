package net.xdob.ratly.server.storage;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.RaftConfiguration;
import net.xdob.ratly.server.config.CorruptionPolicy;
import net.xdob.ratly.server.raftlog.LogProtoUtils;
import net.xdob.ratly.util.AtomicFileOutputStream;
import net.xdob.ratly.util.FileUtils;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.SizeInBytes;

import java.util.Optional;

/** The storage of a {@link RaftServer}. */
public class RaftStorageImpl implements RaftStorage {
	private static final String CHECK_NAME = ".check";
	private static final String JVM_NAME = ManagementFactory.getRuntimeMXBean().getName();

	private final RaftStorageDirectoryImpl storageDir;
  private final StartupOption startupOption;
  private final CorruptionPolicy logCorruptionPolicy;
  private volatile StorageState state = StorageState.UNINITIALIZED;
  private final MetaFile metaFile = new MetaFile();
  private final File dirCache;
  private final RaftPeerId peerId;
  private ExecutorService executor;
  RaftStorageImpl(File dir, SizeInBytes freeSpaceMin, StartupOption option, CorruptionPolicy logCorruptionPolicy, File dirCache, RaftPeerId peerId) {
		LOG.debug("newRaftStorage: {}, freeSpaceMin={}, option={}, logCorruptionPolicy={}, peerId={}",
        dir, freeSpaceMin, option, logCorruptionPolicy, peerId);
    this.storageDir = new RaftStorageDirectoryImpl(dir, freeSpaceMin);
    this.logCorruptionPolicy = Optional.ofNullable(logCorruptionPolicy).orElseGet(CorruptionPolicy::getDefault);
    this.startupOption = option;
    this.dirCache = dirCache;
    this.peerId = peerId;

  }

  public File getDirCache() {
    return dirCache;
  }

  @Override
  public void initialize() throws IOException {
    try {
      if(executor==null) {
        executor = Executors.newSingleThreadExecutor();
      }
      if (startupOption == StartupOption.FORMAT) {
        if (storageDir.analyzeStorage(false) == StorageState.NON_EXISTENT) {
          throw new IOException("Cannot format " + storageDir);
        }
        storageDir.lock();
        format();
        state = storageDir.analyzeStorage(false);
      } else {
        // metaFile is initialized here
        state = analyzeAndRecoverStorage(true);
      }
    } catch (Throwable t) {
      unlockOnFailure(storageDir);
      throw t;
    }

    if (state != StorageState.NORMAL) {
      unlockOnFailure(storageDir);
      throw new IOException("Failed to load " + storageDir + ": " + state);
    }
  }

  @Override
  public boolean checkHealth() {
    if(executor==null) {
      return checkStorageHealth();
    }else{
      Future<Boolean> future = executor.submit(this::checkStorageHealth);
      boolean health = false;
			try {
        health = future.get(2, TimeUnit.SECONDS);
			} catch (Exception ignore) {
        future.cancel(true);
			}
      return health;
		}
  }

	private boolean checkStorageHealth() {
		if(!checkCacheDirHealth()){
			LOG.warn("Cache dir health check failed");
			return false;
		}
		if(!storageDir.checkHealth()){
			LOG.warn("Storage dir health check failed");
			return false;
		}
		return true;
	}

	boolean checkCacheDirHealth() {
		File checkFile = getCacheDirCheckFile();
		try(RandomAccessFile file = new RandomAccessFile(checkFile, "rws")) {
			FileLock fileLock = file.getChannel().tryLock();
			if(fileLock!=null) {
				file.write(JVM_NAME.getBytes(StandardCharsets.UTF_8));
				fileLock.close();
				return true;
			}
		}  catch (Exception e) {
			LOG.warn("checkHealth {} failed.", this.getDirCache(), e);
		}
		return false;
	}

	File getCacheDirCheckFile() {
		return Paths.get(dirCache.toString(),
				this.storageDir.getRoot().getName(),
				peerId + CHECK_NAME).toFile();
	}

	static void unlockOnFailure(RaftStorageDirectoryImpl dir) {
    try {
      dir.unlock();
    } catch (Throwable t) {
      LOG.warn("Failed to unlock " + dir, t);
    }
  }

  StorageState getState() {
    return state;
  }

  public CorruptionPolicy getLogCorruptionPolicy() {
    return logCorruptionPolicy;
  }

  private void format() throws IOException {
    storageDir.clearDirectory();
    metaFile.set(storageDir.getMetaFile()).persist(RaftStorageMetadata.getDefault());
    LOG.info("Storage directory {} has been successfully formatted.", storageDir.getRoot());
  }

  private void cleanMetaTmpFile() throws IOException {
    FileUtils.deleteIfExists(storageDir.getMetaTmpFile());
  }

  private StorageState analyzeAndRecoverStorage(boolean toLock) throws IOException {
    StorageState storageState = storageDir.analyzeStorage(toLock);
    // Existence of raft-meta.tmp means the change of votedFor/term has not
    // been committed. Thus we should delete the tmp file.
    if (storageState != StorageState.NON_EXISTENT) {
      cleanMetaTmpFile();
    }
    if (storageState == StorageState.NORMAL) {
      final File f = storageDir.getMetaFile();
      if (!f.exists()) {
        throw new FileNotFoundException("Metadata file " + f + " does not exists.");
      }
      final RaftStorageMetadata metadata = metaFile.set(f).getMetadata();
      LOG.info("Read {} from {}", metadata, f);
      return StorageState.NORMAL;
    } else if (storageState == StorageState.NOT_FORMATTED &&
        storageDir.isCurrentEmpty()) {
      format();
      return StorageState.NORMAL;
    } else {
      return storageState;
    }
  }

  @Override
  public RaftStorageDirectory getStorageDir() {
    return storageDir;
  }

  @Override
  public void close() throws IOException {
    if(executor!=null) {
      this.executor.shutdown();
      this.executor = null;
    }
    storageDir.unlock();
  }

  @Override
  public RaftStorageMetadataFile getMetadataFile() {
    return metaFile.get();
  }

  public void writeRaftConfiguration(LogEntryProto conf) {
    File confFile = storageDir.getMetaConfFile();
    try (OutputStream fio = new AtomicFileOutputStream(confFile)) {
      conf.writeTo(fio);
    } catch (Exception e) {
      LOG.error("Failed writing configuration to file:" + confFile, e);
    }
  }

  public RaftConfiguration readRaftConfiguration() {
    File confFile = storageDir.getMetaConfFile();
    if (!confFile.exists()) {
      return null;
    } else {
      try (InputStream fio = FileUtils.newInputStream(confFile)) {
        LogEntryProto confProto = LogEntryProto.newBuilder().mergeFrom(fio).build();
        return LogProtoUtils.toRaftConfiguration(confProto);
      } catch (Exception e) {
        LOG.error("Failed reading configuration from file:" + confFile, e);
        return null;
      }
    }
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass()) + ":" + getStorageDir();
  }

  static class MetaFile {
    private final AtomicReference<RaftStorageMetadataFileImpl> ref = new AtomicReference<>();

    RaftStorageMetadataFile get() {
      return ref.get();
    }

    RaftStorageMetadataFile set(File file) {
      final RaftStorageMetadataFileImpl impl = new RaftStorageMetadataFileImpl(file);
      ref.set(impl);
      return impl;
    }
  }
}
