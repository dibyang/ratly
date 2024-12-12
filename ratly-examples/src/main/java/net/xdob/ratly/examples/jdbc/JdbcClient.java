package net.xdob.ratly.examples.jdbc;

import net.xdob.ratly.examples.common.SubCommandBase;
import net.xdob.ratly.jdbc.RatlyDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;

/**
 * Client to connect db cluster.
 */
public abstract class JdbcClient extends SubCommandBase {
  static Logger LOG = LoggerFactory.getLogger(JdbcClient.class);

  @Override
  public void run() throws Exception {
    String url = "jdbc:ratly-jdbc:aio:group="+getRaftGroupId()+";peers=" + peers;
    RatlyDriver driver = new RatlyDriver();
    try(Connection connect = driver.connect(url, new Properties())){
      operation(connect);
    }catch (Exception e){
     LOG.warn("", e);
    }
  }

  protected boolean isQuery(String sql){
    String _sql = sql.toLowerCase().trim();
    return _sql.startsWith("select ")
        || _sql.startsWith("show ")
        || _sql.startsWith("script ");
  }

  protected abstract void operation(Connection connect) throws SQLException;
}
