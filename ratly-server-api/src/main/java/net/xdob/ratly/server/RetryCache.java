
package net.xdob.ratly.server;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

import net.xdob.ratly.protocol.ClientInvocationId;
import net.xdob.ratly.protocol.RaftClientReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For a server to store {@link RaftClientReply} futures in order to handle client retires.
 */
public interface RetryCache extends Closeable {
  Logger LOG = LoggerFactory.getLogger(RetryCache.class);

  /**
   * Entry of a {@link RetryCache},
   * where the key is a {@link ClientInvocationId}
   * and the value is a {@link CompletableFuture} of a {@link RaftClientReply}.
   */
  interface Entry {
    /** @return the cached key. */
    ClientInvocationId getKey();

    /** @return the cached value. */
    CompletableFuture<RaftClientReply> getReplyFuture();
  }

  /** The statistics of a {@link RetryCache}. */
  interface Statistics {
    /** @return the approximate number of entries in the cache. */
    long size();

    /** @return the number of cache hit, where a cache hit is a cache lookup returned a cached value. */
    long hitCount();

    /** @return the ratio of hit count to request count. */
    double hitRate();

    /** @return the number of cache miss, where a cache miss is a cache lookup failed to return a cached value. */
    long missCount();

    /** @return the ratio of miss count to request count. */
    double missRate();
  }

  /** @return the cached entry for the given key if it exists; otherwise, return null. */
  Entry getIfPresent(ClientInvocationId key);

  /** @return the statistics of this {@link RetryCache}. */
  Statistics getStatistics();
}
