package net.xdob.ratly.statemachine.impl;


import net.xdob.ratly.protocol.RaftGroupMemberId;

public interface LeaderChangedListener {
  default void notifyLeaderChanged(boolean isLeader){

  }
  default void changeToCandidate(RaftGroupMemberId groupMemberId) {
  }
}
