package net.xdob.ratly.server;

import net.xdob.ratly.util.Finder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TermLeader {
  private final String leaderId;
  private final long term;
  private long index;

  public TermLeader(String leaderId, long term) {
    this.leaderId = leaderId;
    this.term = term;
  }

  public String getLeaderId() {
    return leaderId;
  }

  public long getTerm() {
    return term;
  }

  public long getIndex() {
    return index;
  }

  public TermLeader setIndex(long index) {
    this.index = index;
    return this;
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
    return toToken();
  }

  public String toToken() {
    return term + "," + index + "$" + leaderId;
  }

  public static TermLeader of(long term, String leaderId){
    return new TermLeader(leaderId, term);
  }

  public static TermLeader parse(String token){
    Finder finder = Finder.c(token);
    //兼容老的数据格式 term,leaderId
    if(!token.contains("$")&&token.contains(",")){
      return of(finder.head(",").getValue(Long.class), finder.tail(",").getValue());
    }
    String leaderId = finder.tail("$").getValue();
    Finder head = finder.head("$");
    TermLeader termLeader = of(head.head(",").getValue(Long.class), leaderId);
    termLeader.setIndex(head.tail(",").getValue(Long.class));
    return termLeader;
  }

}
