package net.xdob.jdbc.sql;

import net.xdob.ratly.client.RaftClient;

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

  public RaftClient getClient() {
    return client;
  }

  public JdbcConnectionInfo getCi() {
    return ci;
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
