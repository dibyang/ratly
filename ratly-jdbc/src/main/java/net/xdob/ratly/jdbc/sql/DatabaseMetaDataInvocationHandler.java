package net.xdob.ratly.jdbc.sql;

import com.google.common.collect.Maps;
import net.xdob.ratly.jdbc.*;
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
    QueryRequest queryRequest = new QueryRequest()
        .setSender(Sender.connection)
        .setType(QueryType.meta)
        .setSession(client.getConnection().getSession())
        .setTx(client.getTx())
        .setDb(client.getCi().getDb())
        .setSql(method.getName());

    queryRequest.getParams().getParameters().add(new Parameter(1).setValue(args));

    return sendQuery(queryRequest);
  }


  protected SerialResultSet sendQuery(QueryRequest queryRequest) throws SQLException {
    QueryReply queryReply = sendQueryRequest(queryRequest);
    if(queryReply.getEx()==null) {
      SerialResultSet rs = (SerialResultSet) queryReply.getRs();
      rs.resetResult();
      return rs;
    }else{
      throw queryReply.getEx();
    }
  }

  protected QueryReply sendQueryRequest(QueryRequest queryRequest) throws SQLException {
    try {
      WrapRequestProto msgProto = WrapRequestProto.newBuilder()
          .setType(DBSMPlugin.DB)
          .setMsg(client.getFasts().asByteString(queryRequest))
          .build();
      RaftClientReply reply =
          client.getClient().io().sendReadOnly(Message.valueOf(msgProto));
      WrapReplyProto replyProto = WrapReplyProto.parseFrom(reply.getMessage().getContent());
      if(!replyProto.getEx().isEmpty()){
        throw (SQLException) client.getFasts().as(replyProto.getEx());
      }
      return client.getFasts().as(replyProto.getRelay());
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
