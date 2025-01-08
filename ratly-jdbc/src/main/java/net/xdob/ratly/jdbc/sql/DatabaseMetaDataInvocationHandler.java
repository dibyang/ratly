package net.xdob.ratly.jdbc.sql;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import net.xdob.ratly.jdbc.DBSMPlugin;
import net.xdob.ratly.jdbc.Version;
import net.xdob.ratly.proto.jdbc.*;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftClientReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class DatabaseMetaDataInvocationHandler implements InvocationHandler {
  static Logger LOG = LoggerFactory.getLogger(DatabaseMetaDataInvocationHandler.class);
  private final Version driverVersion = Version.CURRENT;
  private SqlClient client;

  private static Map<Method, Method> localMethods = Maps.newHashMap();

  public DatabaseMetaDataInvocationHandler(SqlClient client) {
    this.client = client;
  }

  public String getDatabaseProductName() throws SQLException {
    return "ratly-jdbc";
  }

  public String getDatabaseProductVersion() throws SQLException {
    return "1.0";
  }

  public String getDriverName() throws SQLException {
    return "net.xdob.ratly.jdbc.RatlyDriver";
  }

  public String getDriverVersion() throws SQLException {
    return driverVersion.toString();
  }

  public String getURL() throws SQLException {
    return client.getCi().getUrl();
  }

  public int getDriverMajorVersion() {
    return driverVersion.getMajor();
  }

  public int getDriverMinorVersion() {
    return driverVersion.getMinor();
  }



  public ResultSet queryMeta(Method method, Object... args) throws SQLException {
    QueryRequestProto.Builder builder = QueryRequestProto.newBuilder()
        .setSender(Sender.connection)
        .setType(QueryType.meta)
        .setTx(client.getTx())
        .setDb(client.getCi().getDb())
        .setSql(method.getName());
    ParamListProto.Builder ParamListBuilder = ParamListProto.newBuilder();
    ParamListBuilder
        .addParam(ParamProto.newBuilder().setIndex(1)
            .setValue(ByteString.copyFrom(client.getFasts().asByteArray(args))));
    builder.setParam(ParamListBuilder);
    return sendQuery(builder.build());
  }


  protected SerialResultSet sendQuery(QueryRequestProto queryRequest) throws SQLException {
    QueryReplyProto queryReplyProto = sendQueryRequest(queryRequest);
    if(!queryReplyProto.hasEx()) {
      SerialResultSet rs = (SerialResultSet) client.getFasts().asObject(queryReplyProto.getRs().toByteArray());
      rs.resetResult();
      return rs;
    }else{
      throw client.getSQLException(queryReplyProto.getEx());
    }
  }

  protected QueryReplyProto sendQueryRequest(QueryRequestProto queryRequest) throws SQLException {
    try {
      WrapMsgProto msgProto = WrapMsgProto.newBuilder()
          .setType(DBSMPlugin.DB)
          .setMsg(queryRequest.toByteString())
          .build();
      RaftClientReply reply =
          client.getClient().io().sendReadOnly(Message.valueOf(msgProto));
      return QueryReplyProto.parseFrom(reply.getMessage().getContent());
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    if(!localMethods.containsKey(method)) {
      try{
        Method m = this.getClass().getMethod(method.getName(), method.getParameterTypes());
        localMethods.put(method, m);
      }catch(NoSuchMethodException e) {
        localMethods.put(method, null);
      }
    }
    Method m = localMethods.get(method);
    if(m!=null){
      return m.invoke(this, args);
    }else{
      ResultSet resultSet = queryMeta(method, args);
      if (ResultSet.class.isAssignableFrom(method.getReturnType())) {
        return resultSet;
      } else {
        resultSet.next();
        if (method.getReturnType().equals(Boolean.class)
            ||method.getReturnType().equals(boolean.class)) {
          return resultSet.getBoolean(1);
        } else if (method.getReturnType().equals(Integer.class)
            ||method.getReturnType().equals(int.class)) {
          return resultSet.getInt(1);
        } else if (method.getReturnType().equals(Long.class)
            ||method.getReturnType().equals(long.class)) {
          return resultSet.getLong(1);
        } else if (method.getReturnType().equals(String.class)) {
          return resultSet.getString(1);
        } else {
          return resultSet.getString(1);
        }
      }
    }
  }

}
