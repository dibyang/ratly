package net.xdob.ratly.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Simple general resource leak detector using {@link ReferenceQueue} and {@link WeakReference} to
 * observe resource object life-cycle and assert proper resource closure before they are GCed.
 *
 * <p>
 * Example usage:
 *
 * <pre> {@code
 * class MyResource implements AutoClosable {
 *   static final LeakDetector LEAK_DETECTOR = new LeakDetector("MyResource");
 *
 *   private final UncheckedAutoCloseable leakTracker = LEAK_DETECTOR.track(this, () -> {
 *      // report leaks, don't refer to the original object (MyResource) here.
 *      System.out.println("MyResource is not closed before being discarded.");
 *   });
 *
 *   @Override
 *   public void close() {
 *     // proper resources cleanup...
 *     // inform tracker that this object is closed properly.
 *     leakTracker.close();
 *   }
 * }
 *
 * }</pre>
 */
public class LeakDetector {
  private static final Logger LOG = LoggerFactory.getLogger(LeakDetector.class);

  private static class LeakTrackerSet {
    private final Set<LeakTracker> set = Collections.newSetFromMap(new HashMap<>());

    synchronized boolean remove(LeakTracker tracker) {
      return set.remove(tracker);
    }

    synchronized void removeExisting(LeakTracker tracker) {
      final boolean removed = set.remove(tracker);
      Preconditions.assertTrue(removed, () -> "Failed to remove existing " + tracker);
    }

    synchronized LeakTracker add(Object referent, ReferenceQueue<Object> queue, Supplier<String> leakReporter) {
      final LeakTracker tracker = new LeakTracker(referent, queue, this::removeExisting, leakReporter);
      final boolean added = set.add(tracker);
      Preconditions.assertTrue(added, () -> "Failed to add " + tracker + " for " + referent);
      return tracker;
    }

    synchronized int getNumLeaks(boolean throwException) {
      if (set.isEmpty()) {
        return 0;
      }

      int n = 0;
      for (LeakTracker tracker : set) {
        if (tracker.reportLeak() != null) {
          n++;
        }
      }
      if (throwException) {
        assertNoLeaks(n);
      }
      return n;
    }

    synchronized void assertNoLeaks(int leaks) {
      Preconditions.assertTrue(leaks == 0, () -> {
        final int size = set.size();
        return "#leaks = " + leaks + " > 0, #leaks " + (leaks == size? "==" : "!=") + " set.size = " + size;
      });
    }
  }

  private static final AtomicLong COUNTER = new AtomicLong();

  private final ReferenceQueue<Object> queue = new ReferenceQueue<>();
  /** All the {@link LeakTracker}s. */
  private final LeakTrackerSet trackers = new LeakTrackerSet();
  /** When a leak is discovered, a message is printed and added to this list. */
  private final List<String> leakMessages = Collections.synchronizedList(new ArrayList<>());
  private final String name;

  LeakDetector(String name) {
    this.name = name + COUNTER.getAndIncrement();
  }

  LeakDetector start() {
    Thread t = new Thread(this::run);
    t.setName(LeakDetector.class.getSimpleName() + "-" + name);
    t.setDaemon(true);
    LOG.info("Starting leak detector thread {}.", name);
    t.start();
    return this;
  }

  private void run() {
    while (true) {
      try {
        LeakTracker tracker = (LeakTracker) queue.remove();
        // Original resource already been GCed, if tracker is not closed yet,
        // report a leak.
        if (trackers.remove(tracker)) {
          final String leak = tracker.reportLeak();
          if (leak != null) {
            leakMessages.add(leak);
            LOG.warn("#leaks = {}", leak);
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Thread interrupted, exiting.", e);
        break;
      }
    }

    LOG.warn("Exiting leak detector {}.", name);
  }

  Runnable track(Object leakable, Supplier<String> reportLeak) {
    //TODO：可以在此处放置一个速率过滤器，以仅跟踪所有对象的子集，例如 5%、10%、
    //如果我们有证据表明泄漏跟踪会影响性能，或者单个 LeakDetector  thread 跟不上对象分配的步伐。
    //目前，它看起来足够有效，暂时保持简单。
    return trackers.add(leakable, queue, reportLeak)::remove;
  }

  public void assertNoLeaks(int maxRetries, TimeDuration retrySleep) throws InterruptedException {
    synchronized (leakMessages) {
      // leakMessages are all the leaks discovered so far.
      Preconditions.assertTrue(leakMessages.isEmpty(),
          () -> "#leaks = " + leakMessages.size() + "\n" + leakMessages);
    }

    for(int i = 0; i < maxRetries; i++) {
      final int numLeaks = trackers.getNumLeaks(false);
      if (numLeaks == 0) {
        return;
      }
      LOG.warn("{}/{}) numLeaks == {} > 0, will wait and retry ...", i, maxRetries, numLeaks);
      retrySleep.sleep();
    }
    trackers.getNumLeaks(true);
  }

  private static final class LeakTracker extends WeakReference<Object> {
    private final Consumer<LeakTracker> removeMethod;
    private final Supplier<String> getLeakMessage;

    LeakTracker(Object referent, ReferenceQueue<Object> referenceQueue,
        Consumer<LeakTracker> removeMethod, Supplier<String> getLeakMessage) {
      super(referent, referenceQueue);
      this.removeMethod = removeMethod;
      this.getLeakMessage = getLeakMessage;
    }

    /** 当对象完全释放时，由被跟踪的资源调用。*/
    void remove() {
      removeMethod.accept(this);
    }

    /** @return 泄漏消息（如果有泄漏）;如果没有泄漏，则返回 null。 */
    String reportLeak() {
      return getLeakMessage.get();
    }
  }
}
