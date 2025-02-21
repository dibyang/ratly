package net.xdob.ratly.server;


import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

public interface StateObserver extends Closeable {
  void start();
  void notifyTeamIndex(String groupId, TermLeader termIndex);
  CompletableFuture<TermLeader> getLastLeaderTerm(String groupId, int waitMS);
}
