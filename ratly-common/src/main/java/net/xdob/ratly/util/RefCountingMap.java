package net.xdob.ratly.util;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A map of K to V, but does ref counting for added and removed values. The values are
 * not added directly, but instead requested from the given Supplier if ref count == 0. Each put()
 * call will increment the ref count, and each remove() will decrement it. The values are removed
 * from the map iff ref count == 0.
 */
public final class RefCountingMap<K, V> {
  private static class Payload<V> {
    private final V value;
    private final AtomicInteger refCount = new AtomicInteger();

    Payload(V v) {
      this.value = v;
    }

    V get() {
      return value;
    }

    V increment() {
      return refCount.incrementAndGet() > 0? value: null;
    }

    Payload<V> decrement() {
      return refCount.decrementAndGet() > 0? this: null;
    }
  }

  private final ConcurrentMap<K, Payload<V>> map = new ConcurrentHashMap<>();

  public V put(K k, Supplier<V> supplier) {
    return map.compute(k, (k1, old) -> old != null? old: new Payload<>(supplier.get())).increment();
  }

  public static <V> V get(Payload<V> p) {
    return p == null ? null : p.get();
  }

  public V get(K k) {
    return get(map.get(k));
  }

  /**
   * Decrements the ref count of k, and removes from map if ref count == 0.
   * @param k the key to remove
   * @return the value associated with the specified key or null if key is removed from map.
   */
  public V remove(K k) {
    return get(map.computeIfPresent(k, (k1, v) -> v.decrement()));
  }

  public void clear() {
    map.clear();
  }

  public Set<K> keySet() {
    return map.keySet();
  }

  public Collection<V> values() {
    return map.values().stream().map(Payload::get).collect(Collectors.toList());
  }

  public int size() {
    return map.size();
  }
}
