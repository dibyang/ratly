package net.xdob.ratly.server.impl;

import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.util.Finder;

import java.util.Objects;

public class TermLeader {
  private final RaftPeerId leaderId;
  private final long term;
  private long index;

  public TermLeader(RaftPeerId leaderId, long term) {
    this.leaderId = leaderId;
    this.term = term;
  }

  public RaftPeerId getLeaderId() {
    return leaderId;
  }

  public long getTerm() {
    return term;
  }

  public long getIndex() {
    return index;
  }

  public void setIndex(long index) {
    this.index = index;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    TermLeader that = (TermLeader) o;
    return term == that.term && Objects.equals(leaderId, that.leaderId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(leaderId, term);
  }

  @Override
  public String toString() {
    return term + "," + leaderId;
  }

  public static TermLeader of(long term, RaftPeerId leaderId){
    return new TermLeader(leaderId, term);
  }
  public static TermLeader of(long term, String leaderId){
    return of(term, RaftPeerId.getRaftPeerId(leaderId));
  }

  public static TermLeader parse(String token){
    Finder finder = Finder.c(token);
    return of(finder.head(",").getValue(Long.class), finder.tail(",").getValue());
  }
}
