package net.xdob.ratly.examples.jdbc;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.proto.jdbc.*;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftClientReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

/**
 *
 */
@Parameters(commandDescription = "send update sql.")
public class Update extends SQLClient {
  static Logger LOG = LoggerFactory.getLogger(Update.class);

  @Parameter(names = {
      "--sql"}, description = "update sql")
  private String sql = "CREATE TABLE user2 (username VARCHAR(50) NOT NULL PRIMARY KEY, password VARCHAR(255) NOT NULL," +
      "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, status INT DEFAULT 1);";

  @Override
  protected void operation(RaftClient client) throws IOException {
    LOG.info("send update sql={}", sql);

    UpdateRequestProto.Builder builder = UpdateRequestProto.newBuilder()
        .setSender(Sender.prepared).setSql(sql);
    UpdateRequestProto updateRequestProto = builder.build();
    RaftClientReply reply =
        client.io().send(Message.valueOf(updateRequestProto));
    UpdateReplyProto updateReplyProto = UpdateReplyProto.parseFrom(reply.getMessage().getContent());
    if(!updateReplyProto.hasEx()) {
      LOG.info("num={}",updateReplyProto.getCount());

    }else{
      SQLException e = getSQLException(updateReplyProto.getEx());
      LOG.info("sql error", e);
    }
  }
}
