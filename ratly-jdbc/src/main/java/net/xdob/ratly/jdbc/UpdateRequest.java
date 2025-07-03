package net.xdob.ratly.jdbc;

import net.xdob.ratly.jdbc.sql.JdbcSavepoint;
import net.xdob.ratly.jdbc.sql.Parameters;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class UpdateRequest implements Serializable {
  private String db;
  private UpdateType type;
  private Sender  sender;
  private String session;
  private String password;
  private String tx;
  private String sql;
  private final Parameters params = new Parameters();
  private final List<String> batchSql = new ArrayList<>();
  private final List<Parameters> batchParams = new ArrayList<>();
  private JdbcSavepoint savepoint;

  public String getDb() {
    return db;
  }

  public UpdateRequest setDb(String db) {
    this.db = db;
    return this;
  }

  public UpdateType getType() {
    return type;
  }

  public UpdateRequest setType(UpdateType type) {
    this.type = type;
    return this;
  }

  public Sender getSender() {
    return sender;
  }

  public UpdateRequest setSender(Sender sender) {
    this.sender = sender;
    return this;
  }

  public String getPassword() {
    return password;
  }

  public UpdateRequest setPassword(String password) {
    this.password = password;
    return this;
  }

  public String getSession() {
    return session;
  }

  public UpdateRequest setSession(String session) {
    this.session = session;
    return this;
  }

  public String getTx() {
    return tx;
  }

  public UpdateRequest setTx(String tx) {
    this.tx = tx;
    return this;
  }

  public String getSql() {
    return sql;
  }

  public UpdateRequest setSql(String sql) {
    this.sql = sql;
    return this;
  }

  public Parameters getParams() {
    return params;
  }

  public UpdateRequest addParams(Parameters parameters) {
    this.params.addAll(parameters);
    return this;
  }

  public List<String> getBatchSql() {
    return batchSql;
  }

  public List<Parameters> getBatchParams() {
    return batchParams;
  }

  public UpdateRequest addBatchParams(Parameters parameters) {
    this.batchParams.add(parameters);
    return this;
  }

  public JdbcSavepoint getSavepoint() {
    return savepoint;
  }

  public UpdateRequest setSavepoint(JdbcSavepoint savepoint) {
    this.savepoint = savepoint;
    return this;
  }
}
