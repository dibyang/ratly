package net.xdob.ratly.server;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * 状态观察者（旁观者），用于辅助选主决策
 */
public interface StateObserver {
  /**
   * leader的默认属性key
   */
  String LEADER = "leader";
  /**
   * term的默认属性key
   */
  String TERM = "term";
  /**
   * index的默认属性key
   */
  String INDEX = "index";

  /**
   * 获取观察者名称
   * @return 观察者名称
   */
  String getName();
  default void start(ScheduledExecutorService scheduled){};

  default void stop() {};

  default boolean  isStarted(){
    return false;
  }

  void notifyTeamIndex(String groupId, TermLeader termLeader);

  CompletableFuture<TermLeader> getLastLeaderTerm(String groupId, int waitMS);

}
