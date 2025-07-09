package net.xdob.ratly.jdbc.sql;

import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.protocol.SerialSupport;

import java.sql.SQLException;

public class SqlClient {
  private RaftClient client;
  private JdbcConnection connection;
  protected final JdbcConnectionInfo ci;

  public SqlClient(RaftClient client, JdbcConnection connection, JdbcConnectionInfo ci) {
    this.client = client;
    this.connection = connection;
    this.ci = ci;
  }

  public String getTx(){
    return connection.getTx();
  }

  public int addAndGetUpdateCount(int delta){
    return connection.getUpdateCount().addAndGet(delta);
  }

  public RaftClient getClient() {
    return client;
  }

  public JdbcConnectionInfo getCi() {
    return ci;
  }

  public SerialSupport getFasts() {
    return connection.getFasts();
  }

  public JdbcConnection getConnection() {
    return connection;
  }


  public void close() throws SQLException {
    if(connection!=null){
      connection = null;
    }
  }
}
