package net.xdob.ratly.jdbc;

import net.xdob.ratly.jdbc.sql.JdbcSavepoint;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class UpdateReply implements Serializable {
  private SQLException ex;
  private Long count;
  private ResultSet rs;
  private final List<Long> counts =  new ArrayList<>();
  private final List<ResultSet> rsList =  new ArrayList<>();
  private JdbcSavepoint savepoint;
  private String sessionId;

  public SQLException getEx() {
    return ex;
  }

  public UpdateReply setEx(SQLException ex) {
    this.ex = ex;
    return this;
  }

  public Long getCount() {
    return count;
  }

  public UpdateReply setCount(Long count) {
    this.count = count;
    return this;
  }

  public ResultSet getRs() {
    return rs;
  }

  public UpdateReply setRs(ResultSet rs) {
    this.rs = rs;
    return this;
  }

  public List<Long> getCounts() {
    return counts;
  }

  public List<ResultSet> getRsList() {
    return rsList;
  }

  public JdbcSavepoint getSavepoint() {
    return savepoint;
  }

  public UpdateReply setSavepoint(JdbcSavepoint savepoint) {
    this.savepoint = savepoint;
    return this;
  }

  public String getSessionId() {
    return sessionId;
  }

  public UpdateReply setSessionId(String sessionId) {
    this.sessionId = sessionId;
    return this;
  }
}
