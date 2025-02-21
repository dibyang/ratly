package net.xdob.ratly.statemachine.impl;


public interface LeaderChangedListener {
  default void notifyLeaderChanged(boolean isLeader){

  }
}
