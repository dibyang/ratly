
package net.xdob.ratly.server;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

import net.xdob.ratly.protocol.ClientInvocationId;
import net.xdob.ratly.protocol.RaftClientReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 存储与客户端请求相关的 CompletableFuture<RaftClientReply>，它提供了缓存操作和缓存统计功能。
 * For a server to store {@link RaftClientReply} futures in order to handle client retires.
 */
public interface RetryCache extends Closeable {
  Logger LOG = LoggerFactory.getLogger(RetryCache.class);

  /**
   * 代表 RetryCache 中的一个缓存条目，
   * 它由一个键（{@link ClientInvocationId}）和一个值（CompletableFuture<RaftClientReply>）组成。
   */
  interface Entry {
    /**
     * 返回缓存条目的键，即 ClientInvocationId，该 ID 唯一标识客户端请求。
     */
    ClientInvocationId getKey();

    /**
     * 返回一个 CompletableFuture<RaftClientReply>，用于异步获取客户端请求的响应。
     */
    CompletableFuture<RaftClientReply> getReplyFuture();
  }

  /**
   * 提供 RetryCache 的统计信息，帮助了解缓存的效率和健康状况。
   */
  interface Statistics {
    /**
     * 返回缓存中条目的近似数量。
     * @return the approximate number of entries in the cache.
     */
    long size();

    /**
     * 返回缓存命中的次数，命中是指查询返回了缓存的值。
     * @return the number of cache hit, where a cache hit is a cache lookup returned a cached value.
     */
    long hitCount();

    /**
     * 返回缓存的命中率，即命中次数与总请求次数的比率。
     * @return the ratio of hit count to request count.
     */
    double hitRate();

    /**
     * 返回缓存未命中的次数，未命中是指查询时没有找到缓存的值。
     * @return the number of cache miss, where a cache miss is a cache lookup failed to return a cached value.
     */
    long missCount();

    /**
     * 返回缓存未命中的比率，即未命中次数与总请求次数的比率。
     * @return the ratio of miss count to request count.
     */
    double missRate();
  }

  /**
   * 描述：根据给定的 ClientInvocationId 键查找缓存中是否存在对应的条目。
   * 实现：如果缓存中存在该键的条目，返回相应的 Entry；如果不存在，则返回 null。
   * 用途：用于查询缓存中是否已经存在该客户端请求的结果，从而避免重复处理同一个请求。特别适用于客户端请求的重试处理。
   * @return the cached entry for the given key if it exists; otherwise, return null.
   */
  Entry getIfPresent(ClientInvocationId key);

  /**
   * 描述：获取当前 RetryCache 的统计信息。
   * 返回值：返回 Statistics 对象，包含了缓存的统计数据（命中率、未命中次数等）。
   * 用途：通过缓存统计，可以评估缓存的效率，查看命中率、未命中率以及缓存大小等信息，帮助优化缓存策略。
   * @return the statistics of this {@link RetryCache}.
   */
  Statistics getStatistics();
}
