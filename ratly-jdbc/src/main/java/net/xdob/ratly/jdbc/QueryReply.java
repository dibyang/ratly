package net.xdob.ratly.jdbc;

import net.xdob.ratly.jdbc.sql.SerialParameterMetaData;
import net.xdob.ratly.jdbc.sql.SerialResultSetMetaData;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

public class QueryReply implements Serializable {
  private SQLException ex;
  private Object rs;
  private SerialParameterMetaData paramMeta;
  private SerialResultSetMetaData rsMeta;

  public SQLException getEx() {
    return ex;
  }

  public QueryReply setEx(SQLException ex) {
    this.ex = ex;
    return this;
  }

  public Object getRs() {
    return rs;
  }

  public QueryReply setRs(Object rs) {
    this.rs = rs;
    return this;
  }

  public SerialParameterMetaData getParamMeta() {
    return paramMeta;
  }

  public QueryReply setParamMeta(SerialParameterMetaData paramMeta) {
    this.paramMeta = paramMeta;
    return this;
  }

  public SerialResultSetMetaData getRsMeta() {
    return rsMeta;
  }

  public QueryReply setRsMeta(SerialResultSetMetaData rsMeta) {
    this.rsMeta = rsMeta;
    return this;
  }
}
