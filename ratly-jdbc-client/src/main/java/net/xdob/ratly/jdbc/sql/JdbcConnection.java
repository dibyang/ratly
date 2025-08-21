package net.xdob.ratly.jdbc.sql;

import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.grpc.GrpcFactory;
import net.xdob.ratly.jdbc.proto.SqlExConverter;
import net.xdob.ratly.proto.jdbc.*;
import net.xdob.ratly.proto.sm.WrapReplyProto;
import net.xdob.ratly.proto.sm.WrapRequestProto;
import net.xdob.ratly.protocol.*;
import net.xdob.ratly.retry.RetryPolicies;
import net.xdob.ratly.retry.RetryPolicy;
import net.xdob.ratly.security.RsaHelper;
import net.xdob.ratly.util.TimeDuration;
import org.h2.message.DbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class JdbcConnection implements Connection {
  static final Logger LOG = LoggerFactory.getLogger(JdbcConnection.class);
  public static final String DB = "db";

  private final String url;
  private final JdbcConnectionInfo ci;
  private final RaftClient client;
  //private final SerialSupport fasts = new FastsImpl();
  private boolean autoCommit;
  private String tx = UUID.randomUUID().toString();
  private final AtomicInteger updateCount = new AtomicInteger();
  private final Properties properties = new Properties();
  private final RsaHelper rsaHelper = new RsaHelper();
  private final AtomicReference<String> sessionId = new AtomicReference<>();

  public JdbcConnection(JdbcConnectionInfo ci) throws SQLException {
    this.url = ci.getUrl();
    this.ci = ci;
    RaftProperties raftProperties = new RaftProperties();
    final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ci.getGroup()),
        ci.getPeers());
    RaftClient.Builder builder =
        RaftClient.newBuilder().setProperties(raftProperties);
    builder.setRaftGroup(raftGroup);
    RetryPolicy retryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(20,
        TimeDuration.ONE_SECOND);
    builder.setRetryPolicy(retryPolicy);
    builder.setClientRpc(new GrpcFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), raftProperties));
    client = builder.build();
    sessionId.set(openSession());
    LOG.info("open session: {}", sessionId.get());
  }

  private String openSession() throws SQLException {
    ConnRequestProto.Builder connReqBuilder = newConnRequestBuilder(ConnRequestType.openSession);
    JdbcRequestProto requestProto = JdbcRequestProto.newBuilder()
        .setDb(ci.getDb())
        .setConnRequest(connReqBuilder.build())
        .setOpenSession(OpenSessionProto.newBuilder()
            .setUser(ci.getUser())
            .setPassword(rsaHelper.encrypt(ci.getPassword())))
        .build();
    JdbcResponseProto responseProto = send(requestProto);

    return responseProto.getSessionId();
  }


  public String getSession() {
    return sessionId.get();
  }

  public AtomicInteger getUpdateCount() {
    return updateCount;
  }


  public String getTx() {
    return tx;
  }

  @Override
  public Statement createStatement() throws SQLException {
    return new JdbcStatement(getSqlClient());
  }

  private SqlClient getSqlClient() {
    return new SqlClient(client, this, ci);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return new JdbcPreparedStatement(getSqlClient(), sql);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw DbException.getUnsupportedException("CallableStatement");
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    return "";
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    if(autoCommit){
      if(!getAutoCommit()){
        this.commit();
      }
      this.autoCommit = true;
      this.tx = "";
    }else if(getAutoCommit()){
      this.autoCommit = false;
      this.tx = UUID.randomUUID().toString();
    }
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return autoCommit;
  }

  @Override
  public void commit() throws SQLException {
    this.checkClose();
    if(updateCount.get()>0) {
      ConnRequestProto.Builder connReqBuilder = newConnRequestBuilder(ConnRequestType.commit);
      JdbcRequestProto requestProto = newJdbcRequestBuilder()
          .setConnRequest(connReqBuilder.build())
          .build();
      send(requestProto);
      this.tx = UUID.randomUUID().toString();
      updateCount.set(0);
    }
  }


  protected void checkClose() throws SQLException {
    if(isClosed()){
      throw new SQLException("connection is closed.");
    }
  }


  private JdbcRequestProto.Builder newJdbcRequestBuilder() {
		return JdbcRequestProto.newBuilder()
				.setDb(ci.getDb())
				.setSessionId(sessionId.get());
  }
  private  ConnRequestProto.Builder newConnRequestBuilder(ConnRequestType type) {
    ConnRequestProto.Builder builder = ConnRequestProto.newBuilder()
        .setType( type);
    builder.setTx(tx);
    return builder;
  }

  protected JdbcResponseProto send(JdbcRequestProto request) throws SQLException {
    try {
      WrapRequestProto wrap = WrapRequestProto.newBuilder()
          .setType(JdbcConnection.DB)
          .setJdbcRequest(request)
          .build();
      RaftClientReply reply =
          client.io().send(Message.valueOf(wrap));
      WrapReplyProto replyProto = WrapReplyProto.parseFrom(reply.getMessage().getContent());
      JdbcResponseProto response = replyProto.getJdbcResponse();
      if(response.hasEx()){
        throw SqlExConverter.fromProto(response.getEx());
      }
      return response;
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }


  @Override
  public void rollback() throws SQLException {
    this.checkClose();
    if(updateCount.get()>0) {
      ConnRequestProto.Builder connReqBuilder = newConnRequestBuilder(ConnRequestType.rollback);
      JdbcRequestProto requestProto = newJdbcRequestBuilder()
          .setConnRequest(connReqBuilder.build())
          .build();
      send(requestProto);
      this.tx = UUID.randomUUID().toString();
      updateCount.set(0);
    }
  }


  @Override
  public void close() throws SQLException {
    if(!this.autoCommit){
      try {
        rollback();
      }catch (SQLException e){
        // ignore SQLException
      }
    }
    sessionId.getAndUpdate(s->{
      if(s!=null) {
        try {
          ConnRequestProto.Builder connReqBuilder = newConnRequestBuilder(ConnRequestType.closeSession);
          JdbcRequestProto requestProto = newJdbcRequestBuilder()
              .setSessionId(s)
              .setConnRequest(connReqBuilder.build())
              .build();
          send(requestProto);
        } catch (Exception e) {
          // ignore SQLException
        }
      }
      return null;
    });


    try {
      client.close();
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public boolean isClosed() throws SQLException {
    return client.isClosed();
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    DatabaseMetaDataInvocationHandler handler = new DatabaseMetaDataInvocationHandler(getSqlClient());
    return (DatabaseMetaData)Proxy.newProxyInstance(this.getClass().getClassLoader(),
        new Class[]{DatabaseMetaData.class}, handler);
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {

  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {

  }

  @Override
  public String getCatalog() throws SQLException {
    return "";
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {

  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return 0;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {

  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    return createStatement();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    return prepareStatement(sql);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    return prepareCall(sql);
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    return Collections.emptyMap();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

  }

  @Override
  public void setHoldability(int holdability) throws SQLException {

  }

  @Override
  public int getHoldability() throws SQLException {
    return 0;
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    return setSavepoint(null);
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    this.checkClose();
    ConnRequestProto.Builder connReqBuilder = newConnRequestBuilder(ConnRequestType.savepoint);
    connReqBuilder.setSavepoint(JdbcSavepoint.toProto(new JdbcSavepoint(0, name)));
    JdbcRequestProto requestProto = newJdbcRequestBuilder()
        .setConnRequest(connReqBuilder.build())
        .build();
    JdbcResponseProto responseProto = send(requestProto);
    return JdbcSavepoint.from(responseProto.getSavepoint());
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    this.checkClose();
    ConnRequestProto.Builder connReqBuilder = newConnRequestBuilder(ConnRequestType.rollbackSavepoint);
    JdbcRequestProto requestProto = newJdbcRequestBuilder()
        .setConnRequest(connReqBuilder.build())
        .build();
    send(requestProto);
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    this.checkClose();
    ConnRequestProto.Builder connReqBuilder = newConnRequestBuilder(ConnRequestType.releaseSavepoint);
    connReqBuilder.setSavepoint(JdbcSavepoint.toProto(savepoint));
    JdbcRequestProto requestProto = newJdbcRequestBuilder()
        .setConnRequest(connReqBuilder.build())
        .build();
    send(requestProto);
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return createStatement();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return prepareStatement(sql);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return prepareCall(sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    return prepareStatement(sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    return prepareStatement(sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    return prepareStatement(sql);
  }

  @Override
  public Clob createClob() throws SQLException {
    return null;
  }

  @Override
  public Blob createBlob() throws SQLException {
    return null;
  }

  @Override
  public NClob createNClob() throws SQLException {
    return null;
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    return null;
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return !isClosed();
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    properties.setProperty(name, value);
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    properties.putAll(properties);
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    return properties.getProperty(name);
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return null;
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    return null;
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    return null;
  }

  @Override
  public void setSchema(String schema) throws SQLException {

  }

  @Override
  public String getSchema() throws SQLException {
    return "";
  }

  @Override
  public void abort(Executor executor) throws SQLException {

  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    return 0;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
}
