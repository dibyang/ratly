package net.xdob.ratly.jdbc;

import java.sql.Connection;

public class Session {
  private final String user;
  private final String id;
  private transient Connection connection;
  private transient String tx;

  public Session(String id, String user) {
    this.user = user;
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public String getUser() {
    return user;
  }

  public Connection getConnection() {
    return connection;
  }

  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  public String getTx() {
    return tx;
  }

  public void setTx(String tx) {
    this.tx = tx;
  }
}
