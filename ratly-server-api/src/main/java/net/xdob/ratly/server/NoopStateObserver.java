package net.xdob.ratly.server;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class NoopStateObserver implements StateObserver{
  @Override
  public void start() {

  }

  @Override
  public void notifyTeamIndex(String groupId, TermLeader termIndex) {

  }

  @Override
  public CompletableFuture<TermLeader> getLastLeaderTerm(String groupId, int waitMS) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void close() throws IOException {

  }
}
