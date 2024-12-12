package net.xdob.ratly.examples.jdbc;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.jdbc.sql.SimpleResultSet;
import net.xdob.ratly.proto.jdbc.*;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftClientReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 *
 */
@Parameters(commandDescription = "send query sql.")
public class Query extends SQLClient {
  static Logger LOG = LoggerFactory.getLogger(Query.class);

  @Parameter(names = {
      "--sql"}, description = "query sql")
  private String sql = "show tables";

  @Override
  protected void operation(RaftClient client) throws IOException {
    LOG.info("send sql={}", sql);

    QueryRequestProto queryRequest = QueryRequestProto.newBuilder()
        .setSender(Sender.prepared)
        .setType(QueryType.query).setSql(sql)
        .build();
    RaftClientReply reply =
        client.io().sendReadOnly(Message.valueOf(queryRequest));
    QueryReplyProto queryReplyProto = QueryReplyProto.parseFrom(reply.getMessage().getContent());
    if(!queryReplyProto.hasEx()) {
      SimpleResultSet rs = (SimpleResultSet) fst.asObject(queryReplyProto.getRs().toByteArray());
      rs.resetResult();
      ResultSetMetaData metaData = rs.getMetaData();
      try {
        LOG.info("rs={}", rs);
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
      } catch (SQLException e) {
        LOG.warn("", e);
      }

    }else{
      SQLException e = getSQLException(queryReplyProto.getEx());
      LOG.info("sql error", e);
    }
  }
}
