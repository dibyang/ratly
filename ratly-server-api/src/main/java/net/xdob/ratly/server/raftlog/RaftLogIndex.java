
package net.xdob.ratly.server.raftlog;

import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.StringUtils;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongUnaryOperator;

/**
 * Indices of a raft log such as commit index, match index, etc.
 *
 * This class is thread safe.
 */
public class RaftLogIndex {
  private final Object name;
  private final AtomicLong index;

  public RaftLogIndex(Object name, long initialValue) {
    this.name = name;
    this.index = new AtomicLong(initialValue);
  }

  public long get() {
    return index.get();
  }

  public boolean setUnconditionally(long newIndex, Consumer<Object> log) {
    final long old = index.getAndSet(newIndex);
    final boolean updated = old != newIndex;
    if (updated) {
      log.accept(StringUtils.stringSupplierAsObject(
          () -> name + ": setUnconditionally " + old + " -> " + newIndex));
    }
    return updated;
  }

  public boolean updateUnconditionally(LongUnaryOperator update, Consumer<Object> log) {
    final long old = index.getAndUpdate(update);
    final long newIndex = update.applyAsLong(old);
    final boolean updated = old != newIndex;
    if (updated) {
      log.accept(StringUtils.stringSupplierAsObject(
          () -> name + ": updateUnconditionally " + old + " -> " + newIndex));
    }
    return updated;
  }

  public boolean updateIncreasingly(long newIndex, Consumer<Object> log) {
    final long old = index.getAndSet(newIndex);
    Preconditions.assertTrue(old <= newIndex,
        () -> "Failed to updateIncreasingly for " + name + ": " + old + " -> " + newIndex);
    final boolean updated = old != newIndex;
    if (updated) {
      log.accept(StringUtils.stringSupplierAsObject(
          () -> name + ": updateIncreasingly " + old + " -> " + newIndex));
    }
    return updated;
  }

  public boolean updateToMax(long newIndex, Consumer<Object> log) {
    final long old = index.getAndUpdate(oldIndex -> Math.max(oldIndex, newIndex));
    final boolean updated = old < newIndex;
    if (updated) {
      log.accept(StringUtils.stringSupplierAsObject(
          () -> name + ": updateToMax old=" + old + ", new=" + newIndex + ", updated? " + updated));
    }
    return updated;
  }

  public long incrementAndGet(Consumer<Object> log) {
    final long newIndex = index.incrementAndGet();
    log.accept(StringUtils.stringSupplierAsObject(
        () -> name + ": incrementAndGet " + (newIndex-1) + " -> " + newIndex));
    return newIndex;
  }

  @Override
  public String toString() {
    return name + ":" + index;
  }
}
