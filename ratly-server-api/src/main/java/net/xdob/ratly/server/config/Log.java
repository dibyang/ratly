package net.xdob.ratly.server.config;

import net.xdob.ratly.conf.ConfUtils;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.util.SizeInBytes;
import net.xdob.ratly.util.TimeDuration;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static net.xdob.ratly.conf.ConfUtils.*;
import static net.xdob.ratly.conf.ConfUtils.getInt;

/**
 * 日志配置接口，提供了一系列与日志存储、日志处理以及日志回放等相关的设置和方法。
 */
public interface Log {
  String PREFIX = RaftServerConfigKeys.PREFIX + ".log";

  String USE_MEMORY_KEY = PREFIX + ".use.memory";
  boolean USE_MEMORY_DEFAULT = false;

  static boolean useMemory(RaftProperties properties) {
    return getBoolean(properties::getBoolean, USE_MEMORY_KEY, USE_MEMORY_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setUseMemory(RaftProperties properties, boolean useMemory) {
    setBoolean(properties::setBoolean, USE_MEMORY_KEY, useMemory);
  }

  String QUEUE_ELEMENT_LIMIT_KEY = PREFIX + ".queue.element-limit";
  int QUEUE_ELEMENT_LIMIT_DEFAULT = 4096;

  static int queueElementLimit(RaftProperties properties) {
    return ConfUtils.getInt(properties::getInt, QUEUE_ELEMENT_LIMIT_KEY, QUEUE_ELEMENT_LIMIT_DEFAULT, RaftServerConfigKeys.getDefaultLog(),
        requireMin(1));
  }

  static void setQueueElementLimit(RaftProperties properties, int queueSize) {
    setInt(properties::setInt, QUEUE_ELEMENT_LIMIT_KEY, queueSize, requireMin(1));
  }

  String QUEUE_BYTE_LIMIT_KEY = PREFIX + ".queue.byte-limit";
  SizeInBytes QUEUE_BYTE_LIMIT_DEFAULT = SizeInBytes.valueOf("64MB");

  static SizeInBytes queueByteLimit(RaftProperties properties) {
    return getSizeInBytes(properties::getSizeInBytes,
        QUEUE_BYTE_LIMIT_KEY, QUEUE_BYTE_LIMIT_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  @Deprecated
  static void setQueueByteLimit(RaftProperties properties, int queueSize) {
    setInt(properties::setInt, QUEUE_BYTE_LIMIT_KEY, queueSize, requireMin(1));
  }

  static void setQueueByteLimit(RaftProperties properties, SizeInBytes byteLimit) {
    setSizeInBytes(properties::set, QUEUE_BYTE_LIMIT_KEY, byteLimit, requireMin(1L));
  }

  String PURGE_GAP_KEY = PREFIX + ".purge.gap";
  int PURGE_GAP_DEFAULT = 1024;

  static int purgeGap(RaftProperties properties) {
    return ConfUtils.getInt(properties::getInt, PURGE_GAP_KEY, PURGE_GAP_DEFAULT, RaftServerConfigKeys.getDefaultLog(), requireMin(1));
  }

  static void setPurgeGap(RaftProperties properties, int purgeGap) {
    setInt(properties::setInt, PURGE_GAP_KEY, purgeGap, requireMin(1));
  }

  // Config to allow purging up to the snapshot index even if some other
  // peers are behind in their commit index.
  String PURGE_UPTO_SNAPSHOT_INDEX_KEY = PREFIX + ".purge.upto.snapshot.index";
  boolean PURGE_UPTO_SNAPSHOT_INDEX_DEFAULT = false;

  static boolean purgeUptoSnapshotIndex(RaftProperties properties) {
    return getBoolean(properties::getBoolean, PURGE_UPTO_SNAPSHOT_INDEX_KEY,
        PURGE_UPTO_SNAPSHOT_INDEX_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setPurgeUptoSnapshotIndex(RaftProperties properties, boolean shouldPurgeUptoSnapshotIndex) {
    setBoolean(properties::setBoolean, PURGE_UPTO_SNAPSHOT_INDEX_KEY, shouldPurgeUptoSnapshotIndex);
  }

  String PURGE_PRESERVATION_LOG_NUM_KEY = PREFIX + ".purge.preservation.log.num";
  long PURGE_PRESERVATION_LOG_NUM_DEFAULT = 0L;

  static long purgePreservationLogNum(RaftProperties properties) {
    return getLong(properties::getLong, PURGE_PRESERVATION_LOG_NUM_KEY,
        PURGE_PRESERVATION_LOG_NUM_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setPurgePreservationLogNum(RaftProperties properties, long purgePreserveLogNum) {
    setLong(properties::setLong, PURGE_PRESERVATION_LOG_NUM_KEY, purgePreserveLogNum);
  }

  String SEGMENT_SIZE_MAX_KEY = PREFIX + ".segment.size.max";
  SizeInBytes SEGMENT_SIZE_MAX_DEFAULT = SizeInBytes.valueOf("32MB");

  static SizeInBytes segmentSizeMax(RaftProperties properties) {
    return getSizeInBytes(properties::getSizeInBytes,
        SEGMENT_SIZE_MAX_KEY, SEGMENT_SIZE_MAX_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setSegmentSizeMax(RaftProperties properties, SizeInBytes segmentSizeMax) {
    setSizeInBytes(properties::set, SEGMENT_SIZE_MAX_KEY, segmentSizeMax);
  }

  /**
   * Besides the open segment, the max number of segments caching log entries.
   */
  String SEGMENT_CACHE_NUM_MAX_KEY = PREFIX + ".segment.cache.num.max";
  int SEGMENT_CACHE_NUM_MAX_DEFAULT = 6;

  static int segmentCacheNumMax(RaftProperties properties) {
    return ConfUtils.getInt(properties::getInt, SEGMENT_CACHE_NUM_MAX_KEY,
        SEGMENT_CACHE_NUM_MAX_DEFAULT, RaftServerConfigKeys.getDefaultLog(), requireMin(0));
  }

  static void setSegmentCacheNumMax(RaftProperties properties, int maxCachedSegmentNum) {
    setInt(properties::setInt, SEGMENT_CACHE_NUM_MAX_KEY, maxCachedSegmentNum);
  }

  String SEGMENT_CACHE_SIZE_MAX_KEY = PREFIX + ".segment.cache.size.max";
  SizeInBytes SEGMENT_CACHE_SIZE_MAX_DEFAULT = SizeInBytes.valueOf("200MB");

  static SizeInBytes segmentCacheSizeMax(RaftProperties properties) {
    return getSizeInBytes(properties::getSizeInBytes, SEGMENT_CACHE_SIZE_MAX_KEY,
        SEGMENT_CACHE_SIZE_MAX_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setSegmentCacheSizeMax(RaftProperties properties, SizeInBytes maxCachedSegmentSize) {
    setSizeInBytes(properties::set, SEGMENT_CACHE_SIZE_MAX_KEY, maxCachedSegmentSize);
  }

  String PREALLOCATED_SIZE_KEY = PREFIX + ".preallocated.size";
  SizeInBytes PREALLOCATED_SIZE_DEFAULT = SizeInBytes.valueOf("4MB");

  static SizeInBytes preallocatedSize(RaftProperties properties) {
    return getSizeInBytes(properties::getSizeInBytes,
        PREALLOCATED_SIZE_KEY, PREALLOCATED_SIZE_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setPreallocatedSize(RaftProperties properties, SizeInBytes preallocatedSize) {
    setSizeInBytes(properties::set, PREALLOCATED_SIZE_KEY, preallocatedSize);
  }

  String WRITE_BUFFER_SIZE_KEY = PREFIX + ".write.buffer.size";
  SizeInBytes WRITE_BUFFER_SIZE_DEFAULT = SizeInBytes.valueOf("8MB");

  static SizeInBytes writeBufferSize(RaftProperties properties) {
    return getSizeInBytes(properties::getSizeInBytes,
        WRITE_BUFFER_SIZE_KEY, WRITE_BUFFER_SIZE_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setWriteBufferSize(RaftProperties properties, SizeInBytes writeBufferSize) {
    setSizeInBytes(properties::set, WRITE_BUFFER_SIZE_KEY, writeBufferSize);
  }

  String FORCE_SYNC_NUM_KEY = PREFIX + ".force.sync.num";
  int FORCE_SYNC_NUM_DEFAULT = 128;

  static int forceSyncNum(RaftProperties properties) {
    return ConfUtils.getInt(properties::getInt,
        FORCE_SYNC_NUM_KEY, FORCE_SYNC_NUM_DEFAULT, RaftServerConfigKeys.getDefaultLog(), requireMin(0));
  }

  static void setForceSyncNum(RaftProperties properties, int forceSyncNum) {
    setInt(properties::setInt, FORCE_SYNC_NUM_KEY, forceSyncNum);
  }

  /**
   * Unsafe-flush allow increasing flush index without waiting the actual flush to complete.
   */
  String UNSAFE_FLUSH_ENABLED_KEY = PREFIX + ".unsafe-flush.enabled";
  boolean UNSAFE_FLUSH_ENABLED_DEFAULT = false;

  static boolean unsafeFlushEnabled(RaftProperties properties) {
    return getBoolean(properties::getBoolean,
        UNSAFE_FLUSH_ENABLED_KEY, UNSAFE_FLUSH_ENABLED_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setUnsafeFlushEnabled(RaftProperties properties, boolean unsafeFlush) {
    setBoolean(properties::setBoolean, UNSAFE_FLUSH_ENABLED_KEY, unsafeFlush);
  }

  /**
   * Async-flush will increase flush index until the actual flush has completed.
   */
  String ASYNC_FLUSH_ENABLED_KEY = PREFIX + ".async-flush.enabled";
  boolean ASYNC_FLUSH_ENABLED_DEFAULT = false;

  static boolean asyncFlushEnabled(RaftProperties properties) {
    return getBoolean(properties::getBoolean,
        ASYNC_FLUSH_ENABLED_KEY, ASYNC_FLUSH_ENABLED_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setAsyncFlushEnabled(RaftProperties properties, boolean asyncFlush) {
    setBoolean(properties::setBoolean, ASYNC_FLUSH_ENABLED_KEY, asyncFlush);
  }

  /**
   * Log metadata can guarantee that a server can recover commit index and state machine
   * even if a majority of servers are dead by consuming a little extra space.
   */
  String LOG_METADATA_ENABLED_KEY = PREFIX + ".log-metadata.enabled";
  boolean LOG_METADATA_ENABLED_DEFAULT = true;

  static boolean logMetadataEnabled(RaftProperties properties) {
    return getBoolean(properties::getBoolean,
        LOG_METADATA_ENABLED_KEY, LOG_METADATA_ENABLED_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setLogMetadataEnabled(RaftProperties properties, boolean logMetadata) {
    setBoolean(properties::setBoolean, LOG_METADATA_ENABLED_KEY, logMetadata);
  }

  /**
   * 日志损坏策略
   * 日志损坏的处理策略
   */
  enum CorruptionPolicy {
    /**
     * Rethrow the exception.
     */
    EXCEPTION,
    /**
     * Print a warn log message and return all uncorrupted log entries up to the corruption.
     */
    WARN_AND_RETURN;

    public static CorruptionPolicy getDefault() {
      return EXCEPTION;
    }

    public static <T> CorruptionPolicy get(T supplier, Function<T, CorruptionPolicy> getMethod) {
      return Optional.ofNullable(supplier).map(getMethod).orElse(getDefault());
    }
  }

  String CORRUPTION_POLICY_KEY = PREFIX + ".corruption.policy";
  CorruptionPolicy CORRUPTION_POLICY_DEFAULT = CorruptionPolicy.getDefault();

  static CorruptionPolicy corruptionPolicy(RaftProperties properties) {
    return get(properties::getEnum,
        CORRUPTION_POLICY_KEY, CORRUPTION_POLICY_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setCorruptionPolicy(RaftProperties properties, CorruptionPolicy corruptionPolicy) {
    set(properties::setEnum, CORRUPTION_POLICY_KEY, corruptionPolicy);
  }

  interface StateMachineData {
    String PREFIX = Log.PREFIX + ".statemachine.data";

    String SYNC_KEY = PREFIX + ".sync";
    boolean SYNC_DEFAULT = true;

    static boolean sync(RaftProperties properties) {
      return getBoolean(properties::getBoolean,
          SYNC_KEY, SYNC_DEFAULT, RaftServerConfigKeys.getDefaultLog());
    }

    static void setSync(RaftProperties properties, boolean sync) {
      setBoolean(properties::setBoolean, SYNC_KEY, sync);
    }

    String CACHING_ENABLED_KEY = PREFIX + ".caching.enabled";
    boolean CACHING_ENABLED_DEFAULT = false;

    static boolean cachingEnabled(RaftProperties properties) {
      return getBoolean(properties::getBoolean,
          CACHING_ENABLED_KEY, CACHING_ENABLED_DEFAULT, RaftServerConfigKeys.getDefaultLog());
    }

    static void setCachingEnabled(RaftProperties properties, boolean enable) {
      setBoolean(properties::setBoolean, CACHING_ENABLED_KEY, enable);
    }

    String SYNC_TIMEOUT_KEY = PREFIX + ".sync.timeout";
    TimeDuration SYNC_TIMEOUT_DEFAULT = TimeDuration.valueOf(10, TimeUnit.SECONDS);

    static TimeDuration syncTimeout(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(SYNC_TIMEOUT_DEFAULT.getUnit()),
          SYNC_TIMEOUT_KEY, SYNC_TIMEOUT_DEFAULT, RaftServerConfigKeys.getDefaultLog());
    }

    static void setSyncTimeout(RaftProperties properties, TimeDuration syncTimeout) {
      setTimeDuration(properties::setTimeDuration, SYNC_TIMEOUT_KEY, syncTimeout);
    }

    /**
     * -1: retry indefinitely
     * 0: no retry
     * >0: the number of retries
     */
    String SYNC_TIMEOUT_RETRY_KEY = PREFIX + ".sync.timeout.retry";
    int SYNC_TIMEOUT_RETRY_DEFAULT = -1;

    static int syncTimeoutRetry(RaftProperties properties) {
      return ConfUtils.getInt(properties::getInt, SYNC_TIMEOUT_RETRY_KEY, SYNC_TIMEOUT_RETRY_DEFAULT, RaftServerConfigKeys.getDefaultLog(),
          requireMin(-1));
    }

    static void setSyncTimeoutRetry(RaftProperties properties, int syncTimeoutRetry) {
      setInt(properties::setInt, SYNC_TIMEOUT_RETRY_KEY, syncTimeoutRetry, requireMin(-1));
    }

    String READ_TIMEOUT_KEY = PREFIX + ".read.timeout";
    TimeDuration READ_TIMEOUT_DEFAULT = TimeDuration.valueOf(1000, TimeUnit.MILLISECONDS);

    static TimeDuration readTimeout(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(READ_TIMEOUT_DEFAULT.getUnit()),
          READ_TIMEOUT_KEY, READ_TIMEOUT_DEFAULT, RaftServerConfigKeys.getDefaultLog());
    }

    static void setReadTimeout(RaftProperties properties, TimeDuration readTimeout) {
      setTimeDuration(properties::setTimeDuration, READ_TIMEOUT_KEY, readTimeout);
    }
  }

  interface Appender {
    String PREFIX = Log.PREFIX + ".appender";

    String BUFFER_ELEMENT_LIMIT_KEY = PREFIX + ".buffer.element-limit";
    /**
     * 0 means no limit.
     */
    int BUFFER_ELEMENT_LIMIT_DEFAULT = 0;

    static int bufferElementLimit(RaftProperties properties) {
      return ConfUtils.getInt(properties::getInt,
          BUFFER_ELEMENT_LIMIT_KEY, BUFFER_ELEMENT_LIMIT_DEFAULT, RaftServerConfigKeys.getDefaultLog(), requireMin(0));
    }

    static void setBufferElementLimit(RaftProperties properties, int bufferElementLimit) {
      setInt(properties::setInt, BUFFER_ELEMENT_LIMIT_KEY, bufferElementLimit);
    }

    String BUFFER_BYTE_LIMIT_KEY = PREFIX + ".buffer.byte-limit";
    SizeInBytes BUFFER_BYTE_LIMIT_DEFAULT = SizeInBytes.valueOf("4MB");

    static SizeInBytes bufferByteLimit(RaftProperties properties) {
      return getSizeInBytes(properties::getSizeInBytes,
          BUFFER_BYTE_LIMIT_KEY, BUFFER_BYTE_LIMIT_DEFAULT, RaftServerConfigKeys.getDefaultLog());
    }

    static void setBufferByteLimit(RaftProperties properties, SizeInBytes bufferByteLimit) {
      setSizeInBytes(properties::set, BUFFER_BYTE_LIMIT_KEY, bufferByteLimit);
    }

    String SNAPSHOT_CHUNK_SIZE_MAX_KEY = PREFIX + ".snapshot.chunk.size.max";
    SizeInBytes SNAPSHOT_CHUNK_SIZE_MAX_DEFAULT = SizeInBytes.valueOf("16MB");

    static SizeInBytes snapshotChunkSizeMax(RaftProperties properties) {
      return getSizeInBytes(properties::getSizeInBytes,
          SNAPSHOT_CHUNK_SIZE_MAX_KEY, SNAPSHOT_CHUNK_SIZE_MAX_DEFAULT, RaftServerConfigKeys.getDefaultLog());
    }

    static void setSnapshotChunkSizeMax(RaftProperties properties, SizeInBytes maxChunkSize) {
      setSizeInBytes(properties::set, SNAPSHOT_CHUNK_SIZE_MAX_KEY, maxChunkSize);
    }

    String INSTALL_SNAPSHOT_ENABLED_KEY = PREFIX + ".install.snapshot.enabled";
    boolean INSTALL_SNAPSHOT_ENABLED_DEFAULT = true;

    static boolean installSnapshotEnabled(RaftProperties properties) {
      return getBoolean(properties::getBoolean,
          INSTALL_SNAPSHOT_ENABLED_KEY, INSTALL_SNAPSHOT_ENABLED_DEFAULT, RaftServerConfigKeys.getDefaultLog());
    }

    static void setInstallSnapshotEnabled(RaftProperties properties, boolean shouldInstallSnapshot) {
      setBoolean(properties::setBoolean, INSTALL_SNAPSHOT_ENABLED_KEY, shouldInstallSnapshot);
    }

    String WAIT_TIME_MIN_KEY = PREFIX + ".wait-time.min";
    TimeDuration WAIT_TIME_MIN_DEFAULT = TimeDuration.ONE_MILLISECOND;

    static TimeDuration waitTimeMin(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(WAIT_TIME_MIN_DEFAULT.getUnit()),
          WAIT_TIME_MIN_KEY, WAIT_TIME_MIN_DEFAULT, RaftServerConfigKeys.getDefaultLog());
    }

    static void setWaitTimeMin(RaftProperties properties, TimeDuration minDuration) {
      setTimeDuration(properties::setTimeDuration, WAIT_TIME_MIN_KEY, minDuration);
    }

    String RETRY_POLICY_KEY = PREFIX + ".retry.policy";
    /**
     * The min wait time as 1ms (0 is not allowed) for first 10,
     * (5 iteration with 2 times grpc client retry)
     * next wait 1sec for next 20 retry (10 iteration with 2 times grpc client)
     * further wait for 5sec for max times ((5sec*980)/2 times ~= 40min)
     */
    String RETRY_POLICY_DEFAULT = "1ms,10, 1s,20, 5s,1000";

    static String retryPolicy(RaftProperties properties) {
      return properties.get(RETRY_POLICY_KEY, RETRY_POLICY_DEFAULT);
    }

    static void setRetryPolicy(RaftProperties properties, String retryPolicy) {
      properties.set(RETRY_POLICY_KEY, retryPolicy);
    }
  }
}
