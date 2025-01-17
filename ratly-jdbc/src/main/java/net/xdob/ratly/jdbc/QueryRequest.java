package net.xdob.ratly.jdbc;

import net.xdob.ratly.jdbc.sql.Parameters;

import java.io.Serializable;
import java.util.ArrayList;

public class QueryRequest implements Serializable {
  private String db;
  private QueryType type;
  private Sender  sender;
  private String session;
  private String tx;
  private String sql;
  private String  user;
  private String  password;
  private int  fetchDirection;
  private int  fetchSize;
  private final Parameters params = new Parameters();

  public String getDb() {
    return db;
  }

  public QueryRequest setDb(String db) {
    this.db = db;
    return this;
  }

  public QueryType getType() {
    return type;
  }

  public QueryRequest setType(QueryType type) {
    this.type = type;
    return this;
  }

  public Sender getSender() {
    return sender;
  }

  public QueryRequest setSender(Sender sender) {
    this.sender = sender;
    return this;
  }

  public String getSession() {
    return session;
  }

  public QueryRequest setSession(String session) {
    this.session = session;
    return this;
  }

  public String getTx() {
    return tx;
  }

  public QueryRequest setTx(String tx) {
    this.tx = tx;
    return this;
  }

  public String getSql() {
    return sql;
  }

  public QueryRequest setSql(String sql) {
    this.sql = sql;
    return this;
  }

  public String getUser() {
    return user;
  }

  public QueryRequest setUser(String user) {
    this.user = user;
    return this;
  }

  public String getPassword() {
    return password;
  }

  public QueryRequest setPassword(String password) {
    this.password = password;
    return this;
  }

  public int getFetchDirection() {
    return fetchDirection;
  }

  public QueryRequest setFetchDirection(int fetchDirection) {
    this.fetchDirection = fetchDirection;
    return this;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  public QueryRequest setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
    return this;
  }

  public QueryRequest addParams(Parameters parameters) {
    this.params.addAll(parameters);
    return this;
  }

  public Parameters getParams() {
    return params;
  }


}
