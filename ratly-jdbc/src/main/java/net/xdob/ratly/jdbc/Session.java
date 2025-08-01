package net.xdob.ratly.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class Session implements AutoCloseable {
  static final Logger LOG = LoggerFactory.getLogger(Session.class);
  private final String db;
  private final String user;
  private final long uid;
  private final String id;
  private final Consumer<String> closed;
  private final ConnSupplier connSupplier;
  private Connection connection;
  private transient String tx;

  public Session(SessionRequest request, ConnSupplier connSupplier, Consumer<String> closed) {
    this.db = request.getDb();
    this.user = request.getUser();
    this.id = request.toSessionId();
    this.uid = request.getUid();
    this.connSupplier = connSupplier;
    this.closed = closed;
  }

  public long getUid() {
    return uid;
  }

  public String getDb() {
    return db;
  }

  public String getId() {
    return id;
  }

  public String getUser() {
    return user;
  }

  public Connection getConnection() throws SQLException {
    if(connection==null){
      connection = connSupplier.getConnection();
      //LOG.info("session open connection, id={}", id);
    }
    return connection;
  }

  /**
   * 没有事务则关闭连接，有则不关闭，等待释放事务后关闭连接
   */
  public void closeConnection() throws SQLException {
    if(!hasTx()) {
      if (connection != null) {
        try {
          connection.rollback();
        } catch (SQLException ignore) {
        }
        if(!connection.isClosed()) {
          connection.close();
        }
        connection = null;
        //LOG.info("session close connection, id={}", id);
      }
    }
  }

  public void releaseTx() throws SQLException {
    this.tx = "";
    closeConnection();
  }

  boolean hasTx(){
    return tx!=null&&!tx.isEmpty();
  }

  public String getTx() {
    return tx;
  }

  public void setTx(String tx) {
    this.tx = tx;
  }

  public void close() throws Exception {
    releaseTx();
    if(closed!=null){
      closed.accept(id);
    }
  }

}
