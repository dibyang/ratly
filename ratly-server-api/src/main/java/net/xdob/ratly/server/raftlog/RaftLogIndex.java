package net.xdob.ratly.server.raftlog;

import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.StringUtils;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongUnaryOperator;

/**
 * 用于管理 Raft 日志中的各种索引（如提交索引、匹配索引等）。
 * 该类的设计目标是线程安全，并且在索引更新时支持日志记录功能。
 * Indices of a raft log such as commit index, match index, etc.
 *
 * This class is thread safe.
 */
public class RaftLogIndex {
  /**
   * 用于标识索引的名称（如 "commitIndex"、"matchIndex" 等）。在日志输出中用于区分具体索引。
   */
  private final Object name;
  /**
   * 使用线程安全的 AtomicLong 来存储索引值，确保多线程环境下的并发更新安全性。
   */
  private final AtomicLong index;

  public RaftLogIndex(Object name, long initialValue) {
    this.name = name;
    this.index = new AtomicLong(initialValue);
  }

  /**
   * 获取索引值
   * 功能：返回当前索引值，线程安全。
   * 场景：获取日志的当前索引，例如用于状态检查或调试。
   */
  public long get() {
    return index.get();
  }

  /**
   * 无条件更新索引
   * 功能：直接将索引设置为指定值 newIndex，并记录日志。
   * 逻辑：
   * 比较旧值和新值是否相同，记录更新日志。
   * 场景：在无约束条件下更新索引值，例如初始化索引。
   */
  public boolean setUnconditionally(long newIndex, Consumer<Object> log) {
    final long old = index.getAndSet(newIndex);
    final boolean updated = old != newIndex;
    if (updated) {
      log.accept(StringUtils.stringSupplierAsObject(
          () -> name + ": setUnconditionally " + old + " -> " + newIndex));
    }
    return updated;
  }

  /**
   * 使用操作函数无条件更新索引
   * 功能：通过指定的更新函数 update 更新索引值，并记录日志。
   * 逻辑：使用 AtomicLong.getAndUpdate 方法应用更新函数。
   * 记录更新前后的值。
   * 场景：需要自定义更新逻辑，例如基于当前值计算新值。
   */
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

  /**
   * 递增更新索引（确保单调递增）
   * 功能：更新索引为 newIndex，仅当 newIndex 大于等于当前值时允许更新。
   * 逻辑：
   * 如果 newIndex 小于当前值，则抛出异常。
   * 否则执行更新，并记录日志。
   * 场景：确保索引值单调递增，如更新提交索引或匹配索引。
   */
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

  /**
   * 更新索引为当前值和新值的最大值
   * 功能：将索引值更新为当前值和 newIndex 中的较大者，并记录日志。
   * 逻辑：
   * 使用 Math.max 计算新值。
   * 如果更新前后的值不相同，记录日志。
   * 场景：在对等节点之间同步索引值时使用。
   */
  public boolean updateToMax(long newIndex, Consumer<Object> log) {
    final long old = index.getAndUpdate(oldIndex -> Math.max(oldIndex, newIndex));
    final boolean updated = old < newIndex;
    if (updated) {
      log.accept(StringUtils.stringSupplierAsObject(
          () -> name + ": updateToMax old=" + old + ", new=" + newIndex + ", updated? " + updated));
    }
    return updated;
  }

  /**
   * 索引自增
   * 功能：将索引值加 1，并返回新的值，同时记录日志。
   * 场景：用于简单的递增操作，如日志条目追加时更新索引。
   */
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
