
package net.xdob.ratly.server.impl;

import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.protocol.exceptions.ReadException;
import net.xdob.ratly.server.config.Read;
import net.xdob.ratly.statemachine.StateMachine;
import net.xdob.ratly.util.TimeDuration;
import net.xdob.ratly.util.TimeoutExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/** For supporting linearizable read. */
class ReadRequests {
  private static final Logger LOG = LoggerFactory.getLogger(ReadRequests.class);

  static class ReadIndexQueue {
    private final TimeoutExecutor scheduler = TimeoutExecutor.getInstance();
    private final NavigableMap<Long, CompletableFuture<Long>> sorted = new TreeMap<>();
    private final TimeDuration readTimeout;

    ReadIndexQueue(TimeDuration readTimeout) {
      this.readTimeout = readTimeout;
    }

    CompletableFuture<Long> add(long readIndex) {
      final CompletableFuture<Long> returned;
      final boolean create;
      synchronized (this) {
        // The same as computeIfAbsent except that it also tells if a new value is created.
        final CompletableFuture<Long> existing = sorted.get(readIndex);
        create = existing == null;
        if (create) {
          returned = new CompletableFuture<>();
          sorted.put(readIndex, returned);
        } else {
          returned = existing;
        }
      }

      if (create) {
        scheduler.onTimeout(readTimeout, () -> handleTimeout(readIndex),
            LOG, () -> "Failed to handle read timeout for index " + readIndex);
      }
      return returned;
    }

    private void handleTimeout(long readIndex) {
      final CompletableFuture<Long> removed;
      synchronized (this) {
        removed = sorted.remove(readIndex);
      }
      if (removed == null) {
        return;
      }
      removed.completeExceptionally(new ReadException("Read timeout " + readTimeout + " for index " + readIndex));
    }


    /** Complete all the entries less than or equal to the given applied index. */
    synchronized void complete(Long appliedIndex) {
      final NavigableMap<Long, CompletableFuture<Long>> headMap = sorted.headMap(appliedIndex, true);
      headMap.values().forEach(f -> f.complete(appliedIndex));
      headMap.clear();
    }
  }

  private final ReadIndexQueue readIndexQueue;
  private final StateMachine stateMachine;

  ReadRequests(RaftProperties properties, StateMachine stateMachine) {
    this.readIndexQueue = new ReadIndexQueue(Read.timeout(properties));
    this.stateMachine = stateMachine;
  }

  Consumer<Long> getAppliedIndexConsumer() {
    return readIndexQueue::complete;
  }

  CompletableFuture<Long> waitToAdvance(long readIndex) {
    final long lastApplied = stateMachine.getLastAppliedTermIndex().getIndex();
    if (lastApplied >= readIndex) {
      return CompletableFuture.completedFuture(lastApplied);
    }
    final CompletableFuture<Long> f = readIndexQueue.add(readIndex);
    final long current = stateMachine.getLastAppliedTermIndex().getIndex();
    if (current > lastApplied) {
      readIndexQueue.complete(current);
    }
    return f;
  }
}
