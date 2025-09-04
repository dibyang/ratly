package net.xdob.jdbc.sql;

import com.google.common.collect.Maps;
import net.xdob.jdbc.Version;
import net.xdob.ratly.proto.jdbc.ResultSetProto;
import net.xdob.ratly.protocol.Value;
import net.xdob.ratly.proto.jdbc.*;
import net.xdob.ratly.proto.sm.WrapReplyProto;
import net.xdob.ratly.proto.sm.WrapRequestProto;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.util.Proto2Util;
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
    DatabaseMetaRequestProto.Builder metaReqBuilder = DatabaseMetaRequestProto.newBuilder();
    metaReqBuilder.setMethod(method.getName());
    for (Class<?> parameterType : method.getParameterTypes()) {
      metaReqBuilder.addParametersTypes(parameterType.getName());
    }
    if(args!=null) {
      for (Object arg : args) {
        metaReqBuilder.addArgs(Value.toValueProto(arg));
      }
    }
    ConnRequestProto.Builder request = ConnRequestProto.newBuilder()
        .setType(ConnRequestType.databaseMeta)
        .setDatabaseMetaRequest(metaReqBuilder);
    JdbcRequestProto.Builder builder = JdbcRequestProto.newBuilder();
    builder.setDb(client.getCi().getDb())
        .setSessionId(client.getConnection().getSession())
        .setTimeoutSeconds(3)
        .setConnRequest(request);
    return sendJdbcRequest(builder.build());
  }


  protected ResultSet sendJdbcRequest(JdbcRequestProto queryRequest) throws SQLException {
    JdbcResponseProto response = sendReadOnly(queryRequest);
    ResultSetProto resultSet = response.getResultSet();
    return SerialResultSet.from(resultSet);
  }

  protected JdbcResponseProto sendReadOnly(JdbcRequestProto request) throws SQLException {
		client.getConnection().checkClose();
		try {
      WrapRequestProto wrap = WrapRequestProto.newBuilder()
          .setType(JdbcConnection.DB)
          .setJdbcRequest(request)
          .build();
      RaftClientReply reply =
          client.getClient().io().sendReadOnly(Message.valueOf(wrap));
      WrapReplyProto replyProto = WrapReplyProto.parseFrom(reply.getMessage().getContent());
      JdbcResponseProto response = replyProto.getJdbcResponse();
      if(response.hasEx()){
        throw Proto2Util.toSQLException(response.getEx());
      }
      return response;
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
