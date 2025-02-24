package net.xdob.ratly.server;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;


public enum StateObserverMgr {
  INSTANCE;
  private final ChainedStateObserver observer = new ChainedStateObserver();
  private volatile ScheduledExecutorService scheduled;

  StateObserverMgr(){

  }

  public StateObserverMgr register(StateObserver stateObserver) {
    observer.register(stateObserver);
    return this;
  }

  public StateObserverMgr unregister(String observerName) {
    observer.unregister(observerName);
    return this;
  }

  public void start(ScheduledExecutorService scheduled){
    observer.start(scheduled);
  };

  public void stop() {
    observer.stop();
    if(scheduled!=null){
      scheduled.shutdown();
      scheduled = null;
    }
  }

  public boolean  isStarted(){
    return observer.isStarted();
  }

  public void notifyTeamIndex(String groupId, TermLeader termLeader){
    observer.notifyTeamIndex(groupId, termLeader);
  }

  public CompletableFuture<TermLeader> getLastLeaderTerm(String groupId, int waitMS){
    return observer.getLastLeaderTerm(groupId, waitMS);
  }

}
