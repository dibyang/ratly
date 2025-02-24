package net.xdob.ratly.server;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChainedStateObserver implements StateObserver {
  static final Logger LOG = LoggerFactory.getLogger(ChainedStateObserver.class);
  private final Map<String,StateObserver> observers = Maps.newConcurrentMap();
  private final AtomicBoolean started = new AtomicBoolean(false);
  private volatile ScheduledExecutorService scheduled;
  public ChainedStateObserver register(StateObserver observer) {
    synchronized (observers) {
      if (!observers.containsKey(observer.getName())) {
        observers.put(observer.getName(), observer);
        if (isStarted()) {
          try {
            observer.start(scheduled);
          } catch (Exception e) {
            LOG.warn("observer start error.", e);
          }
        }
      }
    }
    return this;
  }

  public ChainedStateObserver unregister(String observerName) {
    synchronized (observers) {
      StateObserver observer = observers.remove(observerName);
      if (observer!=null&&observer.isStarted()) {
        try {
          observer.stop();
        } catch (Exception e) {
          LOG.warn("observer stop error.", e);
        }
      }
    }
    return this;
  }

  @Override
  public String getName() {
    return "ChainedStateObserver";
  }

  @Override
  public void start(ScheduledExecutorService scheduled) {
    if(started.compareAndSet(false,true)) {
      this.scheduled = scheduled;
      for (StateObserver observer : observers.values()) {
        try {
          observer.start(scheduled);
        } catch (Exception e) {
          LOG.warn("observer start error.", e);
        }
      }
    }
  }

  @Override
  public void stop() {
    if(started.compareAndSet(true,false)) {
      for (StateObserver observer : observers.values()) {
        CompletableFuture.runAsync(()->{
          try {
            observer.stop();
          } catch (Exception e) {
            LOG.warn("observer stop error.", e);
          }
        });
      }
      this.scheduled = null;
    }
  }

  @Override
  public boolean isStarted() {
    return started.get();
  }

  @Override
  public void notifyTeamIndex(String groupId, TermLeader termIndex) {
    for (StateObserver observer : observers.values()) {
      try {
        observer.notifyTeamIndex(groupId, termIndex);
      } catch (Exception e) {
        LOG.warn("observer notifyTeamIndex error.", e);
      }
    }
  }

  @Override
  public CompletableFuture<TermLeader> getLastLeaderTerm(String groupId, int waitMS) {
    return CompletableFuture.supplyAsync(() -> {
      CountDownLatch latch = new CountDownLatch(observers.size());
      List<TermLeader> termLeaders = new ArrayList<>();
      for (StateObserver observer : observers.values()) {
        try {
          observer.getLastLeaderTerm(groupId, waitMS)
              .whenComplete((r, ex) -> {
                if (ex == null && r != null) {
                  termLeaders.add(r);
                }
                latch.countDown();
              });
        } catch (Exception e) {
          LOG.warn("observer notifyTeamIndex error.", e);
        }
      }
      try {
        latch.await(waitMS, TimeUnit.MILLISECONDS);
      } catch (InterruptedException ignore) {
        //ignore InterruptedException
      }
      long term = termLeaders.stream().mapToLong(TermLeader::getTerm).max().orElse(-1L);
      return termLeaders.stream().filter(e->e.getTerm()== term)
          .max(Comparator.comparingLong(TermLeader::getIndex)).orElse(null);
    });
  }


}
