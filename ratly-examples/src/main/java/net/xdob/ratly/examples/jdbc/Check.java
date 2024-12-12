package net.xdob.ratly.examples.jdbc;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.proto.jdbc.QueryReplyProto;
import net.xdob.ratly.proto.jdbc.QueryRequestProto;
import net.xdob.ratly.proto.jdbc.QueryType;
import net.xdob.ratly.proto.jdbc.Sender;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.fasts.serialization.FSTConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

/**
 *
 */
@Parameters(commandDescription = "send sql for check.")
public class Check extends SQLClient {
  static Logger LOG = LoggerFactory.getLogger(Check.class);

  @Parameter(names = {
      "--sql"}, description = "sql for check")
  private String sql = "select * from users";

  @Override
  protected void operation(RaftClient client) throws IOException {
    LOG.info("send sql={}", sql);

    QueryRequestProto queryRequest = QueryRequestProto.newBuilder()
        .setSender(Sender.prepared)
        .setType(QueryType.check).setSql(sql)
        .build();
    RaftClientReply reply =
        client.io().sendReadOnly(Message.valueOf(queryRequest));
    QueryReplyProto queryReplyProto = QueryReplyProto.parseFrom(reply.getMessage().getContent());
    if(!queryReplyProto.hasEx()) {
      LOG.info("{} is valid",sql);
    }else{
      SQLException e = getSQLException(queryReplyProto.getEx());
      LOG.info("sql error", e);
    }
  }
}
