package net.xdob.ratly.examples.jdbc;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

/**
 *
 */
@Parameters(commandDescription = "Execute sql.")
public class Execute extends JdbcClient {
  static Logger LOG = LoggerFactory.getLogger(Execute.class);

  @Parameter(names = {
      "--sql"}, description = "query sql")
  private String sql = "show tables";

  @Parameter(names = {
      "--params"}, description = "params")
  private String params = "user3";

  @Parameter(names = {
      "--cmd"}, description = "commit or rollback")
  private String cmd = "";

  @Override
  protected void operation(Connection connection) throws SQLException {
    LOG.info("send sql={}", sql);
    try (PreparedStatement ps = connection.prepareStatement(sql)){
      connection.setAutoCommit(false);
      List<String> values = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(params);
      for (int i = 0; i < values.size(); i++) {
        ps.setObject(i+1, values.get(i));
      }
      if(isQuery(sql)){
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        StringBuilder header = new StringBuilder();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
          header.append(metaData.getColumnName(i + 1))
              .append("\t");
        }
        LOG.info(header.toString());
        while(rs.next()){
          StringBuilder row = new StringBuilder();
          for (int i = 0; i < metaData.getColumnCount(); i++) {
            row.append(rs.getString(i+1))
                .append("\t");
          }
          LOG.info(row.toString());
        }
      }else{
        int count = ps.executeUpdate();
        LOG.info("count={}", count);
      }
      if(!Strings.isNullOrEmpty(cmd)){
        if(cmd.equalsIgnoreCase("commit")){
          connection.commit();
        }
        if(cmd.equalsIgnoreCase("rollback")){
          connection.rollback();
        }
      }

    }catch (SQLException e) {
      LOG.warn("", e);
    }
  }
}
