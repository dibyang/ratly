package net.xdob.ratly.server.impl;

import net.xdob.ratly.server.config.RaftServerConfigKeys;
import net.xdob.ratly.util.*;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.proto.raft.ReplicationLevel;
import net.xdob.ratly.proto.raft.WatchRequestTypeProto;
import net.xdob.ratly.protocol.exceptions.NotReplicatedException;
import net.xdob.ratly.protocol.RaftClientRequest;
import net.xdob.ratly.protocol.exceptions.ResourceUnavailableException;
import net.xdob.ratly.server.metrics.RaftServerMetricsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

class WatchRequests {
  public static final Logger LOG = LoggerFactory.getLogger(WatchRequests.class);

  static class PendingWatch {
    private final WatchRequestTypeProto watch;
    private final Timestamp creationTime;
    private final Supplier<CompletableFuture<Long>> future = JavaUtils.memoize(CompletableFuture::new);

    PendingWatch(WatchRequestTypeProto watch, Timestamp creationTime) {
      this.watch = watch;
      this.creationTime = creationTime;
    }

    CompletableFuture<Long> getFuture() {
      return future.get();
    }

    long getIndex() {
      return watch.getIndex();
    }

    Timestamp getCreationTime() {
      return creationTime;
    }

    @Override
    public String toString() {
      return RaftClientRequest.Type.toString(watch) + "@" + creationTime
          + "?" + StringUtils.completableFuture2String(future.get(), true);
    }
  }

  private class WatchQueue {
    private final ReplicationLevel replication;
    private final SortedMap<PendingWatch, PendingWatch> q = new TreeMap<>(
        Comparator.comparingLong(PendingWatch::getIndex).thenComparing(PendingWatch::getCreationTime));
    private final ResourceSemaphore resource;
    private final RaftServerMetricsImpl raftServerMetrics;
    private volatile long index; //Invariant: q.isEmpty() or index < any element q

    WatchQueue(ReplicationLevel replication, int elementLimit, RaftServerMetricsImpl raftServerMetrics) {
      this.replication = replication;
      this.resource = new ResourceSemaphore(elementLimit);
      this.raftServerMetrics = raftServerMetrics;

      raftServerMetrics.addNumPendingWatchRequestsGauge(resource::used, replication);
    }

    long getIndex() {
      return index;
    }

    /**
     * 该函数 add 的功能是将一个 Raft 客户端的 watch 请求加入到 watch 队列中，
     * 并在满足条件或超时后返回结果。具体逻辑如下：
     *   1. 计算当前时间和超时时间：基于系统纳秒时间，计算出向上取整的超时时间节点。
     *   2. 创建 PendingWatch 对象：封装请求和超时时间。
     *   3. 检查是否已满足条件：若当前队列的 index 已大于等于 pending 的 index，则直接返回已完成的 Future。
     *   4. 尝试入队与资源控制：
     *      使用 TreeMap.compute 确保唯一性；
     *      通过 ResourceSemaphore 控制并发资源，若无法获取资源（队列满），则记录指标并返回异常。
     *   5. 处理重复请求：如果该 pending 已存在，则返回其 Future。
     *   6. 注册超时处理：若为新加入项，设置超时回调以触发 handleTimeout 方法。
     *   7. 返回 Future：供调用者监听完成结果。
     */
    CompletableFuture<Long> add(RaftClientRequest request) {
      final long currentTime = Timestamp.currentTimeNanos();
      final long roundUp = watchTimeoutDenominationNanos.roundUpNanos(currentTime);
      final PendingWatch pending = new PendingWatch(request.getType().getWatch(), Timestamp.valueOf(roundUp));

      final PendingWatch computed;
      synchronized (this) {
        final long queueIndex = getIndex();
        if (pending.getIndex() <= queueIndex) { // compare again synchronized
          // watch condition already satisfied
          return CompletableFuture.completedFuture(queueIndex);
        }
        computed = q.compute(pending, (key, old) -> old != null? old: resource.tryAcquire()? pending: null);
      }

      if (computed == null) {
        // failed to acquire
        raftServerMetrics.onWatchRequestQueueLimitHit(replication);
        return JavaUtils.completeExceptionally(new ResourceUnavailableException(
            "Failed to acquire a pending watch request in " + name + " for " + request));
      }
      if (computed != pending) {
        // already exists in q
        return computed.getFuture();
      }

      // newly added to q
      final TimeDuration timeout = watchTimeoutNanos.apply(duration -> duration + roundUp - currentTime);
      scheduler.onTimeout(timeout, () -> handleTimeout(request, pending),
          LOG, () -> name + ": Failed to timeout " + request);
      return pending.getFuture();
    }

    void handleTimeout(RaftClientRequest request, PendingWatch pending) {
      if (removeExisting(pending)) {
        pending.getFuture().completeExceptionally(
            new NotReplicatedException(request.getCallId(), replication, pending.getIndex()));
        LOG.debug("{}: timeout {}, {}", name, pending, request);
        raftServerMetrics.onWatchRequestTimeout(replication);
      }
    }

    synchronized boolean removeExisting(PendingWatch pending) {
      final PendingWatch removed = q.remove(pending);
      if (removed == null) {
        return false;
      }
      Preconditions.assertTrue(removed == pending);
      resource.release();
      return true;
    }

    synchronized void updateIndex(final long newIndex) {
      if (newIndex <= getIndex()) { // compare again synchronized
        return;
      }
      LOG.debug("{}: update {} index from {} to {}", name, replication, index, newIndex);
      index = newIndex;

      for(; !q.isEmpty();) {
        final PendingWatch first = q.firstKey();
        if (first.getIndex() > newIndex) {
          return;
        }
        final boolean removed = removeExisting(first);
        Preconditions.assertTrue(removed);
        LOG.debug("{}: complete {}", name, first);
        first.getFuture().complete(newIndex);
      }
    }

    synchronized void failAll(Exception e) {
      for(PendingWatch pending : q.values()) {
        pending.getFuture().completeExceptionally(e);
      }
      q.clear();
      resource.close();
    }

    void close() {
      if (raftServerMetrics != null) {
        raftServerMetrics.removeNumPendingWatchRequestsGauge(replication);
      }
    }
  }

  private final String name;
  private final Map<ReplicationLevel, WatchQueue> queues = new EnumMap<>(ReplicationLevel.class);

  private final TimeDuration watchTimeoutNanos;
  private final TimeDuration watchTimeoutDenominationNanos;
  private final TimeoutExecutor scheduler = TimeoutExecutor.getInstance();

  WatchRequests(Object name, RaftProperties properties, RaftServerMetricsImpl raftServerMetrics) {
    this.name = name + "-" + JavaUtils.getClassSimpleName(getClass());

    final TimeDuration watchTimeout = RaftServerConfigKeys.Watch.timeout(properties);
    this.watchTimeoutNanos = watchTimeout.to(TimeUnit.NANOSECONDS);
    final TimeDuration watchTimeoutDenomination = RaftServerConfigKeys.Watch.timeoutDenomination(properties);
    this.watchTimeoutDenominationNanos = watchTimeoutDenomination.to(TimeUnit.NANOSECONDS);
    Preconditions.assertTrue(watchTimeoutNanos.getDuration() % watchTimeoutDenominationNanos.getDuration() == 0L,
        () -> "watchTimeout (=" + watchTimeout + ") is not a multiple of watchTimeoutDenomination (="
            + watchTimeoutDenomination + ").");

    final int elementLimit = RaftServerConfigKeys.Watch.elementLimit(properties);
    Arrays.stream(ReplicationLevel.values()).forEach(r -> queues.put(r,
        new WatchQueue(r, elementLimit, raftServerMetrics)));
  }

  /**
   * 该函数用于将客户端的 Watch 请求添加到对应的队列中，并判断是否需要异步等待：
   *   1. 从请求中提取 Watch 类型及其配置（如复制级别 replication）；
   *   2. 根据 replication 找到对应的 WatchQueue 队列；
   *   3. 获取当前队列的最新索引 queueIndex；
   *   4. 如果请求中的索引 watch.getIndex() ≤ queueIndex，表示条件已满足，直接返回已完成的 Future；
   *   5. 否则，将请求加入队列，并返回一个异步 Future，后续会在条件满足时被唤醒。
   */
  CompletableFuture<Long> add(RaftClientRequest request) {
    final WatchRequestTypeProto watch = request.getType().getWatch();
    final WatchQueue queue = queues.get(watch.getReplication());
    final long queueIndex = queue.getIndex();
    if (watch.getIndex() <= queueIndex) { // compare without synchronization
      // watch condition already satisfied
      return CompletableFuture.completedFuture(queueIndex);
    }
    return queue.add(request);
  }

  void update(ReplicationLevel replication, final long newIndex) {
    final WatchQueue queue = queues.get(replication);
    if (newIndex > queue.getIndex()) { // compare without synchronization
      queue.updateIndex(newIndex);
    }
  }

  void failWatches(Exception e) {
    queues.values().forEach(q -> q.failAll(e));
  }

  void close() {
    queues.values().forEach(WatchQueue::close);
  }
}
