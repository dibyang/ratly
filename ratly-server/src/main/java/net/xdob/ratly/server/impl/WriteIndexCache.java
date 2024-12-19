package net.xdob.ratly.server.impl;

import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.protocol.ClientId;
import net.xdob.ratly.protocol.RaftClientRequest;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import net.xdob.ratly.server.config.RaftServerConfigKeys;
import net.xdob.ratly.util.TimeDuration;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/** Caching the per client write index in order to support read-after-write consistency. */
class WriteIndexCache {
  private final Cache<ClientId, AtomicReference<CompletableFuture<Long>>> cache;

  WriteIndexCache(RaftProperties properties) {
    this(RaftServerConfigKeys.Read.ReadAfterWriteConsistent.writeIndexCacheExpiryTime(properties));
  }

  /**
   * @param cacheExpiryTime time for a cache entry to expire.
   */
  WriteIndexCache(TimeDuration cacheExpiryTime) {
    this.cache = CacheBuilder.newBuilder()
        .expireAfterAccess(cacheExpiryTime.getDuration(), cacheExpiryTime.getUnit())
        .build();
  }

  void add(ClientId key, CompletableFuture<Long> future) {
    final AtomicReference<CompletableFuture<Long>> ref;
    try {
      ref = cache.get(key, AtomicReference::new);
    } catch (ExecutionException e) {
      throw new IllegalStateException(e);
    }
    ref.set(future);
  }

  CompletableFuture<Long> getWriteIndexFuture(RaftClientRequest request) {
    if (request != null && request.getType().getRead().getReadAfterWriteConsistent()) {
      final AtomicReference<CompletableFuture<Long>> ref = cache.getIfPresent(request.getClientId());
      if (ref != null) {
        return ref.get();
      }
    }
    return CompletableFuture.completedFuture(null);
  }
}
