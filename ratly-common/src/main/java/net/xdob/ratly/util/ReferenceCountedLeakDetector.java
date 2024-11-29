package net.xdob.ratly.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * 用于检测引用计数对象（@{@link ReferenceCountedObject}）潜在资源泄漏问题的工具，尤其适用于调试和测试环境中。
 * 它通过引用计数的管理和追踪，对资源的分配和释放进行严格的监控，从而有效地发现泄漏的根源。
 */
public final class ReferenceCountedLeakDetector {
  private static final Logger LOG = LoggerFactory.getLogger(ReferenceCountedLeakDetector.class);
  // Leak detection is turned off by default.

  private static final AtomicReference<Mode> FACTORY = new AtomicReference<>(Mode.NONE);
  private static final Supplier<LeakDetector> SUPPLIER
      = MemoizedSupplier.valueOf(() -> new LeakDetector(FACTORY.get().name()).start());

  static Factory getFactory() {
    return FACTORY.get();
  }

  public static LeakDetector getLeakDetector() {
    return SUPPLIER.get();
  }

  private ReferenceCountedLeakDetector() {
  }

  public static synchronized void enable(boolean advanced) {
    FACTORY.set(advanced ? Mode.ADVANCED : Mode.SIMPLE);
  }

  interface Factory {
    <V> ReferenceCountedObject<V> create(V value, Runnable retainMethod, Consumer<Boolean> releaseMethod);
  }

  /**
   * 泄漏检测模式
   */
  private enum Mode implements Factory {
    /**
     * 禁用泄漏检测，仅用于生产环境。
     */
    NONE {
      @Override
      public <V> ReferenceCountedObject<V> create(V value, Runnable retainMethod, Consumer<Boolean> releaseMethod) {
        return new Impl<>(value, retainMethod, releaseMethod);
      }
    },
    /**
     * 基本的泄漏检测，适合一般测试。
     */
    SIMPLE {
      @Override
      public <V> ReferenceCountedObject<V> create(V value, Runnable retainMethod, Consumer<Boolean> releaseMethod) {
        return new SimpleTracing<>(value, retainMethod, releaseMethod, getLeakDetector());
      }
    },
    /**
     * 高级泄漏检测，记录对象创建、每次保留和释放的调用栈，适合用于调试特定问题。
     */
    ADVANCED {
      @Override
      public <V> ReferenceCountedObject<V> create(V value, Runnable retainMethod, Consumer<Boolean> releaseMethod) {
        return new AdvancedTracing<>(value, retainMethod, releaseMethod, getLeakDetector());
      }
    }
  }

  private static class Impl<V> implements ReferenceCountedObject<V> {
    private final AtomicInteger count;
    private final V value;
    private final Runnable retainMethod;
    private final Consumer<Boolean> releaseMethod;

    Impl(V value, Runnable retainMethod, Consumer<Boolean> releaseMethod) {
      this.value = value;
      this.retainMethod = retainMethod;
      this.releaseMethod = releaseMethod;
      count = new AtomicInteger();
    }

    @Override
    public V get() {
      final int previous = count.get();
      if (previous < 0) {
        throw new IllegalStateException("Failed to get: object has already been completely released.");
      } else if (previous == 0) {
        throw new IllegalStateException("Failed to get: object has not yet been retained.");
      }
      return value;
    }

    final int getCount() {
      return count.get();
    }

    @Override
    public V retain() {
      // n <  0: exception
      // n >= 0: n++
      if (count.getAndUpdate(n -> n < 0? n : n + 1) < 0) {
        throw new IllegalStateException("Failed to retain: object has already been completely released.");
      }

      retainMethod.run();
      return value;
    }

    @Override
    public boolean release() {
      // n <= 0: exception
      // n >  1: n--
      // n == 1: n = -1
      final int previous = count.getAndUpdate(n -> n <= 1? -1: n - 1);
      if (previous < 0) {
        throw new IllegalStateException("Failed to release: object has already been completely released.");
      } else if (previous == 0) {
        throw new IllegalStateException("Failed to release: object has not yet been retained.");
      }
      final boolean completedReleased = previous == 1;
      releaseMethod.accept(completedReleased);
      return completedReleased;
    }
  }

  /**
   * 增强了 Impl 的功能，添加了泄漏跟踪机制。
   * 每次引用计数增加到 1 时，调用 LeakDetector.track() 注册对象。
   * 释放时，通过 removeMethod.run() 从泄漏跟踪中移除。
   * 检测未释放的对象并记录相关信息。
   * @param <T>
   */
  private static class SimpleTracing<T> extends Impl<T> {
    private final LeakDetector leakDetector;
    private final Class<?> valueClass;
    private String valueString = null;
    private Runnable removeMethod = null;

    SimpleTracing(T value, Runnable retainMethod, Consumer<Boolean> releaseMethod, LeakDetector leakDetector) {
      super(value, retainMethod, releaseMethod);
      this.valueClass = value.getClass();
      this.leakDetector = leakDetector;
    }

    String getTraceString(int count) {
      return "(" + valueClass + ", count=" + count + ", value=" + valueString + ")";
    }

    /** @return the leak message if there is a leak; return null if there is no leak. */
    String logLeakMessage() {
      final int count = getCount();
      if (count == 0) { // never retain
        return null;
      }
      final String message = "LEAK: " + getTraceString(count);
      LOG.warn(message);
      return message;
    }

    @Override
    public synchronized T get() {
      try {
        return super.get();
      } catch (Exception e) {
        throw new IllegalStateException("Failed to get: " + getTraceString(getCount()), e);
      }
    }

    @Override
    public synchronized T retain() {
      final T value;
      try {
        value = super.retain();
      } catch (Exception e) {
        throw new IllegalStateException("Failed to retain: " + getTraceString(getCount()), e);
      }
      if (getCount() == 1) { // this is the first retain
        this.removeMethod = leakDetector.track(this, this::logLeakMessage);
        this.valueString = value.toString();
      }
      return value;
    }

    @Override
    public synchronized boolean release() {
      final boolean released;
      try {
        released = super.release();
      } catch (Exception e) {
        throw new IllegalStateException("Failed to release: " + getTraceString(getCount()), e);
      }

      if (released) {
        Preconditions.assertNotNull(removeMethod, () -> "Not yet retained (removeMethod == null): " + valueClass);
        removeMethod.run();
      }
      return released;
    }
  }

  /**
   * 扩展了 SimpleTracing，记录详细的调用栈和操作历史。
   * 每次 retain 和 release 都会生成 TraceInfo，记录线程信息、调用栈以及引用计数变化。
   * @param <T>
   */
  private static class AdvancedTracing<T> extends SimpleTracing<T> {
    enum Op {CREATION, RETAIN, RELEASE, CURRENT}

    static class Counts {
      private final int refCount;
      private final int retainCount;
      private final int releaseCount;

      Counts() {
        this.refCount = 0;
        this.retainCount = 0;
        this.releaseCount = 0;
      }

      Counts(Op op, Counts previous) {
        if (op == Op.RETAIN) {
          this.refCount = previous.refCount + 1;
          this.retainCount = previous.retainCount + 1;
          this.releaseCount = previous.releaseCount;
        } else if (op == Op.RELEASE) {
          this.refCount = previous.refCount - 1;
          this.retainCount = previous.retainCount;
          this.releaseCount = previous.releaseCount + 1;
        } else {
          throw new IllegalStateException("Unexpected op: " + op);
        }
      }

      @Override
      public String toString() {
        return "refCount=" + refCount
            + ", retainCount=" + retainCount
            + ", releaseCount=" + releaseCount;
      }
    }

    static class TraceInfo {
      private final int id;
      private final Op op;
      private final int previousRefCount;
      private final Counts counts;

      private final String threadInfo;
      private final StackTraceElement[] stackTraces;
      private final int newTraceElementIndex;

      TraceInfo(int id, Op op, TraceInfo previous, int previousRefCount) {
        this.id = id;
        this.op = op;
        this.previousRefCount = previousRefCount;
        this.counts = previous == null? new Counts()
            : op == Op.CURRENT ? previous.counts
            : new Counts(op, previous.counts);

        final Thread thread = Thread.currentThread();
        this.threadInfo = "Thread_" + thread.getId() + ":" + thread.getName();
        this.stackTraces = thread.getStackTrace();
        this.newTraceElementIndex = previous == null? stackTraces.length - 1
            : findFirstUnequalFromTail(this.stackTraces, previous.stackTraces);
      }

      static <T> int findFirstUnequalFromTail(T[] current, T[] previous) {
        int c = current.length - 1;
        for(int p = previous.length - 1; p >= 0; p--, c--) {
          if (!previous[p].equals(current[c])) {
            return c;
          }
        }
        return -1;
      }

      private StringBuilder appendTo(StringBuilder b) {
        b.append(op).append("_").append(id)
            .append(": previousRefCount=").append(previousRefCount)
            .append(", ").append(counts)
            .append(", ").append(threadInfo).append("\n");
        final int n = newTraceElementIndex + 1;
        int line = 3;
        for (; line <= n && line < stackTraces.length; line++) {
          b.append("    ").append(stackTraces[line]).append("\n");
        }
        if (line < stackTraces.length) {
          b.append("    ...\n");
        }
        return b;
      }

      @Override
      public String toString() {
        return appendTo(new StringBuilder()).toString();
      }
    }

    private final List<TraceInfo> traceInfos = new ArrayList<>();
    private TraceInfo previous;

    AdvancedTracing(T value, Runnable retainMethod, Consumer<Boolean> releaseMethod, LeakDetector leakDetector) {
      super(value, retainMethod, releaseMethod, leakDetector);
      addTraceInfo(Op.CREATION, -1);
    }

    private synchronized TraceInfo addTraceInfo(Op op, int previousRefCount) {
      final TraceInfo current = new TraceInfo(traceInfos.size(), op, previous, previousRefCount);
      traceInfos.add(current);
      previous = current;
      return current;
    }


    @Override
    public synchronized T retain() {
      final int previousRefCount = getCount();
      final T retained = super.retain();
      final TraceInfo info = addTraceInfo(Op.RETAIN, previousRefCount);
      Preconditions.assertSame(getCount(), info.counts.refCount, "refCount");
      return retained;
    }

    @Override
    public synchronized boolean release() {
      final int previousRefCount = getCount();
      final boolean released = super.release();
      final TraceInfo info = addTraceInfo(Op.RELEASE, previousRefCount);
      final int count = getCount();
      final int expected = count == -1? 0 : count;
      Preconditions.assertSame(expected, info.counts.refCount, "refCount");
      return released;
    }

    @Override
    synchronized String getTraceString(int count) {
      return super.getTraceString(count) + getTraceInfosString();
    }

    private String getTraceInfosString() {
      final int n = traceInfos.size();
      final StringBuilder b = new StringBuilder(n << 10).append(" #TraceInfos=").append(n);
      TraceInfo last = null;
      for (TraceInfo info : traceInfos) {
        info.appendTo(b.append("\n"));
        last = info;
      }

      // append current track info
      final TraceInfo current = new TraceInfo(n, Op.CURRENT, last, getCount());
      current.appendTo(b.append("\n"));

      return b.toString();
    }
  }
}
