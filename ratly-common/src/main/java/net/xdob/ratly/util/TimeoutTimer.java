package net.xdob.ratly.util;

import net.xdob.ratly.util.function.CheckedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class TimeoutTimer implements TimeoutExecutor {
  public static final Logger LOG = LoggerFactory.getLogger(TimeoutTimer.class);

  private static final Supplier<TimeoutTimer> INSTANCE = JavaUtils.memoize(() -> new TimeoutTimer(MAXIMUM_POOL_SIZE));

  public static TimeoutTimer getInstance() {
    return INSTANCE.get();
  }

  static class Task extends TimerTask {
    private final int id;
    private final Runnable runnable;

    Task(int id, Runnable runnable) {
      this.id = id;
      this.runnable = LogUtils.newRunnable(LOG, runnable, this::toString);
    }

    @Override
    public void run() {
      LOG.debug("run {}", this);
      runnable.run();
    }

    @Override
    public String toString() {
      return "task #" + id;
    }
  }

  /** The number of scheduled tasks. */
  private final AtomicInteger numTasks = new AtomicInteger();
  /** A unique ID for each task. */
  private final AtomicInteger taskId = new AtomicInteger();

  private final List<MemoizedSupplier<Timer>> timers;

  private TimeoutTimer(int numTimers) {
    final List<MemoizedSupplier<Timer>> list = new ArrayList<>(numTimers);
    for(int i = 0; i < numTimers; i++) {
      final String name = "timer" + i;
      list.add(JavaUtils.memoize(() -> new Timer(name, true)));
    }
    this.timers = Collections.unmodifiableList(list);
  }

  @Override
  public int getTaskCount() {
    return numTasks.get();
  }

  private Timer getTimer(int tid) {
    return timers.get(Math.toIntExact(Integer.toUnsignedLong(tid) % timers.size())).get();
  }

  private void schedule(TimeDuration timeout, Runnable toSchedule) {
    final int tid = taskId.incrementAndGet();
    final int n = numTasks.incrementAndGet();
    LOG.debug("schedule a task #{} with timeout {}, numTasks={}", tid, timeout, n);
    getTimer(n).schedule(new Task(tid, toSchedule), timeout.toLong(TimeUnit.MILLISECONDS));
  }

  @Override
  public <THROWABLE extends Throwable> void onTimeout(
      TimeDuration timeout, CheckedRunnable<THROWABLE> task, Consumer<THROWABLE> errorHandler) {
    schedule(timeout, () -> {
      try {
        task.run();
      } catch(Throwable t) {
        errorHandler.accept(JavaUtils.cast(t));
      } finally {
        numTasks.decrementAndGet();
      }
    });
  }
}
