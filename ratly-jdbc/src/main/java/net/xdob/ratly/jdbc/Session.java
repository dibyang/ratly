package net.xdob.ratly.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class Session {
  static final Logger LOG = LoggerFactory.getLogger(Session.class);
  private final String user;
  private final String id;
  private final Consumer<String> closed;
  private final ConnSupplier connSupplier;
  private Connection connection;
  private transient String tx;
  private transient volatile long accessTime = System.nanoTime();

  public Session(String id, String user, ConnSupplier connSupplier, Consumer<String> closed) {
    this.user = user;
    this.id = id;
    this.connSupplier = connSupplier;
    this.closed = closed;
  }



  /**
   * 更新访问时间
   */
  public void updateAccessTime(){
    accessTime = System.nanoTime();
  }

  /**
   * 获取据上次访问的时长（秒）
   * @return 据上次访问的时长（秒）
   */
  public long getAccessTimeOffset(){
    return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - accessTime);
  }

  public String getId() {
    return id;
  }

  public String getUser() {
    updateAccessTime();
    return user;
  }

  public Connection getConnection() throws SQLException {
    updateAccessTime();
    if(connection==null){
      connection = connSupplier.getConnection();
      //LOG.info("session open connection, id={}", id);
    }
    return connection;
  }

  public void closeConnection() throws SQLException {
    if(connection!=null){
      connection.rollback();
      connection.close();
      connection = null;
      //LOG.info("session close connection, id={}", id);
    }
  }


  public String getTx() {
    updateAccessTime();
    return tx;
  }

  public void setTx(String tx) {
    updateAccessTime();
    this.tx = tx;
  }



  public void close() throws Exception {
    closeConnection();
    if(closed!=null){
      closed.accept(id);
    }
  }
}
