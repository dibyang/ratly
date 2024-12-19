package net.xdob.ratly.jdbc;


public interface LeaderChangedListener {
  default void notifyLeaderChanged(boolean isLeader){

  }
}
