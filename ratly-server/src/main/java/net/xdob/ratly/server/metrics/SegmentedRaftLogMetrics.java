package net.xdob.ratly.server.metrics;

import net.xdob.ratly.metrics.LongCounter;
import net.xdob.ratly.metrics.Timekeeper;
import net.xdob.ratly.protocol.RaftGroupMemberId;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.UncheckedAutoCloseable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class SegmentedRaftLogMetrics extends RaftLogMetricsBase {
  //////////////////////////////
  // Raft Log Write Path Metrics
  /////////////////////////////
  /** Time taken to flush log. */
  public static final String RAFT_LOG_FLUSH_TIME = "flushTime";
  /** Number of times of log flushed. */
  public static final String RAFT_LOG_FLUSH_COUNT = "flushCount";
  /** Time taken to log sync. */
  public static final String RAFT_LOG_SYNC_TIME = "syncTime";
  /** Raft log data queue size which at any time gives the number of log related operations in the queue. */
  public static final String RAFT_LOG_DATA_QUEUE_SIZE = "dataQueueSize";
  /** Raft log worker queue size which at any time gives number of committed entries that are to be synced. */
  public static final String RAFT_LOG_WORKER_QUEUE_SIZE = "workerQueueSize";
  /** Number of raft log entries synced in each flush call. */
  public static final String RAFT_LOG_SYNC_BATCH_SIZE = "syncBatchSize";
  /** Count of RaftLogCache Misses */
  public static final String RAFT_LOG_CACHE_MISS_COUNT = "cacheMissCount";
  /** Count of RaftLogCache Hits */
  public static final String RAFT_LOG_CACHE_HIT_COUNT = "cacheHitCount";
  /** Number of SegmentedRaftLogCache::closedSegments */
  public static final String RAFT_LOG_CACHE_CLOSED_SEGMENTS_NUM = "closedSegmentsNum";
  /** Size of SegmentedRaftLogCache::closedSegments in bytes */
  public static final String RAFT_LOG_CACHE_CLOSED_SEGMENTS_SIZE_IN_BYTES = "closedSegmentsSizeInBytes";
  /** Size of SegmentedRaftLogCache::openSegment in bytes */
  public static final String RAFT_LOG_CACHE_OPEN_SEGMENT_SIZE_IN_BYTES = "openSegmentSizeInBytes";
  /** Total time taken to append a raft log entry */
  public static final String RAFT_LOG_APPEND_ENTRY_LATENCY = "appendEntryLatency";
  /** Time spent by a Raft log operation in the queue. */
  public static final String RAFT_LOG_TASK_QUEUE_TIME = "enqueuedTime";
  /**
   * Time taken for a Raft log operation to get into the queue after being requested.
   * This is the time that it has to wait for the queue to be non-full.
   */
  public static final String RAFT_LOG_TASK_ENQUEUE_DELAY = "queueingDelay";
  /** Time taken for a Raft log operation to complete execution. */
  public static final String RAFT_LOG_TASK_EXECUTION_TIME = "%sExecutionTime";
  /** Number of entries appended to the raft log */
  public static final String RAFT_LOG_APPEND_ENTRY_COUNT = "appendEntryCount";
  public static final String RAFT_LOG_PURGE_METRIC = "purgeLog";
  /** Number of statemachine dataApi write timeouts */
  public static final String RAFT_LOG_STATEMACHINE_DATA_WRITE_TIMEOUT_COUNT = "numStateMachineDataWriteTimeout";
  /** Number of statemachine dataApi read timeouts */
  public static final String RAFT_LOG_STATEMACHINE_DATA_READ_TIMEOUT_COUNT = "numStateMachineDataReadTimeout";

  //////////////////////////////
  // Raft Log Read Path Metrics
  /////////////////////////////
  /** Time required to read a raft log entry from actual raft log file and create a raft log entry */
  public static final String RAFT_LOG_READ_ENTRY_LATENCY = "readEntryLatency";
  /** Time required to load and process raft log segments during restart */
  public static final String RAFT_LOG_LOAD_SEGMENT_LATENCY = "segmentLoadLatency";

  private final Timekeeper flushTimer = getRegistry().timer(RAFT_LOG_FLUSH_TIME);
  private final Timekeeper syncTimer = getRegistry().timer(RAFT_LOG_SYNC_TIME);
  private final Timekeeper enqueuedTimer = getRegistry().timer(RAFT_LOG_TASK_QUEUE_TIME);
  private final Timekeeper queuingDelayTimer = getRegistry().timer(RAFT_LOG_TASK_ENQUEUE_DELAY);

  private final Timekeeper appendEntryTimer = getRegistry().timer(RAFT_LOG_APPEND_ENTRY_LATENCY);
  private final Timekeeper readEntryTimer = getRegistry().timer(RAFT_LOG_READ_ENTRY_LATENCY);
  private final Timekeeper loadSegmentTimer = getRegistry().timer(RAFT_LOG_LOAD_SEGMENT_LATENCY);
  private final Timekeeper purgeTimer = getRegistry().timer(RAFT_LOG_PURGE_METRIC);

  private final LongCounter cacheHitCount = getRegistry().counter(RAFT_LOG_CACHE_HIT_COUNT);
  private final LongCounter cacheMissCount= getRegistry().counter(RAFT_LOG_CACHE_MISS_COUNT);
  private final LongCounter appendEntryCount = getRegistry().counter(RAFT_LOG_APPEND_ENTRY_COUNT);
  private final LongCounter flushCount = getRegistry().counter(RAFT_LOG_FLUSH_COUNT);

  private final LongCounter numStateMachineDataWriteTimeout = getRegistry().counter(
      RAFT_LOG_STATEMACHINE_DATA_WRITE_TIMEOUT_COUNT);
  private final LongCounter numStateMachineDataReadTimeout = getRegistry().counter(
      RAFT_LOG_STATEMACHINE_DATA_READ_TIMEOUT_COUNT);

  private final Map<Class<?>, Timekeeper> taskClassTimers = new ConcurrentHashMap<>();

  public SegmentedRaftLogMetrics(RaftGroupMemberId serverId) {
    super(serverId);
  }

  public void addDataQueueSizeGauge(Supplier<Integer> numElements) {
    getRegistry().gauge(RAFT_LOG_DATA_QUEUE_SIZE, () -> numElements);
  }

  public void addClosedSegmentsNum(Supplier<Long> cachedSegmentNum) {
    getRegistry().gauge(RAFT_LOG_CACHE_CLOSED_SEGMENTS_NUM, () -> cachedSegmentNum);
  }

  public void addClosedSegmentsSizeInBytes(Supplier<Long> closedSegmentsSizeInBytes) {
    getRegistry().gauge(RAFT_LOG_CACHE_CLOSED_SEGMENTS_SIZE_IN_BYTES, () -> closedSegmentsSizeInBytes);
  }

  public void addOpenSegmentSizeInBytes(Supplier<Long> openSegmentSizeInBytes) {
    getRegistry().gauge(RAFT_LOG_CACHE_OPEN_SEGMENT_SIZE_IN_BYTES, () -> openSegmentSizeInBytes);
  }

  public void addLogWorkerQueueSizeGauge(Supplier<Integer> queueSize) {
    getRegistry().gauge(RAFT_LOG_WORKER_QUEUE_SIZE, () -> queueSize);
  }

  public void addFlushBatchSizeGauge(Supplier<Integer> flushBatchSize) {
    getRegistry().gauge(RAFT_LOG_SYNC_BATCH_SIZE, () -> flushBatchSize);
  }

  public UncheckedAutoCloseable startFlushTimer() {
    return Timekeeper.start(flushTimer);
  }

  public Timekeeper getSyncTimer() {
    return syncTimer;
  }

  public void onRaftLogCacheHit() {
    cacheHitCount.inc();
  }

  public void onRaftLogCacheMiss() {
    cacheMissCount.inc();
  }

  public void onRaftLogFlush() {
    flushCount.inc();
  }

  public void onRaftLogAppendEntry() {
    appendEntryCount.inc();
  }

  public Timekeeper.Context startAppendEntryTimer() {
    return appendEntryTimer.time();
  }

  public Timekeeper getEnqueuedTimer() {
    return enqueuedTimer;
  }

  public UncheckedAutoCloseable startQueuingDelayTimer() {
    return Timekeeper.start(queuingDelayTimer);
  }

  private Timekeeper newTaskExecutionTimer(Class<?> taskClass) {
    return getRegistry().timer(String.format(RAFT_LOG_TASK_EXECUTION_TIME,
        JavaUtils.getClassSimpleName(taskClass).toLowerCase()));
  }
  public UncheckedAutoCloseable startTaskExecutionTimer(Class<?> taskClass) {
    return Timekeeper.start(taskClassTimers.computeIfAbsent(taskClass, this::newTaskExecutionTimer));
  }

  public Timekeeper getReadEntryTimer() {
    return readEntryTimer;
  }

  public UncheckedAutoCloseable startLoadSegmentTimer() {
    return Timekeeper.start(loadSegmentTimer);
  }

  public UncheckedAutoCloseable startPurgeTimer() {
    return Timekeeper.start(purgeTimer);
  }

  @Override
  public void onStateMachineDataWriteTimeout() {
    numStateMachineDataWriteTimeout.inc();
  }

  @Override
  public void onStateMachineDataReadTimeout() {
    numStateMachineDataReadTimeout.inc();
  }
}
