package net.xdob.ratly.examples.jdbc;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.jdbc.sql.SimpleParameterMetaData;
import net.xdob.ratly.jdbc.sql.SimpleResultSetMetaData;
import net.xdob.ratly.proto.jdbc.QueryReplyProto;
import net.xdob.ratly.proto.jdbc.QueryRequestProto;
import net.xdob.ratly.proto.jdbc.QueryType;
import net.xdob.ratly.proto.jdbc.Sender;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftClientReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

/**
 *
 */
@Parameters(commandDescription = "send get meta for sql.")
public class Meta extends SQLClient {
  static Logger LOG = LoggerFactory.getLogger(Meta.class);


  @Parameter(names = {
      "--sql"}, description = "sql for get meta")
  private String sql = "select * from user2 where username=?";

  @Override
  protected void operation(RaftClient client) throws IOException {
    LOG.info("send sql={}", sql);

    QueryRequestProto queryRequest = QueryRequestProto.newBuilder()
        .setSender(Sender.prepared)
        .setType(QueryType.meta).setSql(sql)
        .build();
    RaftClientReply reply =
        client.io().sendReadOnly(Message.valueOf(queryRequest));
    QueryReplyProto queryReplyProto = QueryReplyProto.parseFrom(reply.getMessage().getContent());
    if(!queryReplyProto.hasEx()) {

      try {
        SimpleResultSetMetaData rsMeta = (SimpleResultSetMetaData) fst.asObject(queryReplyProto.getRsMeta().toByteArray());

        for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
          LOG.info("{} name={} type={}", i, rsMeta.getColumnName(i), rsMeta.getColumnTypeName(i));
        }

        SimpleParameterMetaData paramMeta = (SimpleParameterMetaData) fst.asObject(queryReplyProto.getParamMeta().toByteArray());
        for (int i = 1; i <= paramMeta.getParameterCount(); i++) {
          LOG.info("{} type={}, type name={}",i,paramMeta.getParameterType(i),paramMeta.getParameterTypeName(i));
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
