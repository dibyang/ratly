package net.xdob.jdbc;

import net.xdob.ratly.server.raftlog.RaftLog;

public class DbState extends DbInfo{

  private volatile long appliedIndex = RaftLog.INVALID_LOG_INDEX;

  public DbState() {
  }

  public DbState(DbInfo dbInfo) {
    super(dbInfo);
  }

  public long getAppliedIndex() {
    return appliedIndex;
  }

  public DbState setAppliedIndex(long appliedIndex) {
    this.appliedIndex = appliedIndex;
    return this;
  }

  public DbInfo toDbInfo(){
    return new DbInfo(this);
  }

  @Override
  public String toString() {
    return "{" +
        "name:'" + getName() + '\'' +
        ", users:" + getUsers() +
        ", appliedIndex:" + appliedIndex +
        '}';
  }

}
