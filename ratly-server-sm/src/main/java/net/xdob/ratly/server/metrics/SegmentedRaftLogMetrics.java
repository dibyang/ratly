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
  // Raft日志写路径指标
  /////////////////////////////
  /** 刷新日志所花费的时间。 */
  public static final String RAFT_LOG_FLUSH_TIME = "flushTime";
  /** 日志刷新次数。 */
  public static final String RAFT_LOG_FLUSH_COUNT = "flushCount";
  /** 日志同步所花费的时间。 */
  public static final String RAFT_LOG_SYNC_TIME = "syncTime";
  /** Raft日志数据队列大小，表示队列中日志相关操作的数量。 */
  public static final String RAFT_LOG_DATA_QUEUE_SIZE = "dataQueueSize";
  /** Raft日志工作队列大小，表示待同步的已提交条目数量。 */
  public static final String RAFT_LOG_WORKER_QUEUE_SIZE = "workerQueueSize";
  /** 每次刷新调用中同步的日志条目数量。 */
  public static final String RAFT_LOG_SYNC_BATCH_SIZE = "syncBatchSize";
  /** RaftLogCache未命中次数 */
  public static final String RAFT_LOG_CACHE_MISS_COUNT = "cacheMissCount";
  /** RaftLogCache命中次数 */
  public static final String RAFT_LOG_CACHE_HIT_COUNT = "cacheHitCount";
  /** SegmentedRaftLogCache::closedSegments的数量 */
  public static final String RAFT_LOG_CACHE_CLOSED_SEGMENTS_NUM = "closedSegmentsNum";
  /** SegmentedRaftLogCache::closedSegments的大小（字节） */
  public static final String RAFT_LOG_CACHE_CLOSED_SEGMENTS_SIZE_IN_BYTES = "closedSegmentsSizeInBytes";
  /** SegmentedRaftLogCache::openSegment的大小（字节） */
  public static final String RAFT_LOG_CACHE_OPEN_SEGMENT_SIZE_IN_BYTES = "openSegmentSizeInBytes";
  /** 追加Raft日志条目所花费的总时间 */
  public static final String RAFT_LOG_APPEND_ENTRY_LATENCY = "appendEntryLatency";
  /** Raft日志操作在队列中花费的时间。 */
  public static final String RAFT_LOG_TASK_QUEUE_TIME = "enqueuedTime";
  /**
   * Raft日志操作在请求后进入队列所花费的时间。
   * 这是它必须等待队列非满的时间。
   */
  public static final String RAFT_LOG_TASK_ENQUEUE_DELAY = "queueingDelay";
  /** Raft日志操作完成执行所花费的时间。 */
  public static final String RAFT_LOG_TASK_EXECUTION_TIME = "%sExecutionTime";
  /** 追加到Raft日志中的条目数量 */
  public static final String RAFT_LOG_APPEND_ENTRY_COUNT = "appendEntryCount";
  public static final String RAFT_LOG_PURGE_METRIC = "purgeLog";
  /** 状态机dataApi写超时次数 */
  public static final String RAFT_LOG_STATEMACHINE_DATA_WRITE_TIMEOUT_COUNT = "numStateMachineDataWriteTimeout";
  /** 状态机dataApi读超时次数 */
  public static final String RAFT_LOG_STATEMACHINE_DATA_READ_TIMEOUT_COUNT = "numStateMachineDataReadTimeout";

  //////////////////////////////
  // Raft日志读路径指标
  /////////////////////////////
  /** 从实际Raft日志文件读取Raft日志条目并创建Raft日志条目所需的时间 */
  public static final String RAFT_LOG_READ_ENTRY_LATENCY = "readEntryLatency";
  /** 在重启期间加载和处理Raft日志段所需的时间 */
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
