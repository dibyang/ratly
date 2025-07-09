package net.xdob.ratly.jdbc.sql;

import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.client.impl.FastsImpl;
import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.grpc.GrpcFactory;
import net.xdob.ratly.jdbc.*;
import net.xdob.ratly.proto.jdbc.*;
import net.xdob.ratly.protocol.*;
import net.xdob.ratly.retry.RetryLimited;
import net.xdob.ratly.retry.RetryPolicies;
import net.xdob.ratly.security.Base58;
import net.xdob.ratly.security.RsaHelper;
import net.xdob.ratly.security.crypto.factory.PasswordEncoderFactories;
import net.xdob.ratly.security.crypto.password.PasswordEncoder;
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

public class JdbcConnection implements Connection {
  static final Logger LOG = LoggerFactory.getLogger(JdbcConnection.class);
  public static final String DB = "db";

  private final String url;
  private final JdbcConnectionInfo ci;
  private final RaftClient client;
  private final SerialSupport fasts = new FastsImpl();
  private boolean autoCommit;
  private String tx = UUID.randomUUID().toString();
  private final AtomicInteger updateCount = new AtomicInteger();
  private final Properties properties = new Properties();
  private final RsaHelper rsaHelper = new RsaHelper();
  private final String session;

  public JdbcConnection(JdbcConnectionInfo ci) throws SQLException {
    this.url = ci.getUrl();
    this.ci = ci;
    RaftProperties raftProperties = new RaftProperties();
    final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ci.getGroup()),
        ci.getPeers());
    RaftClient.Builder builder =
        RaftClient.newBuilder().setProperties(raftProperties);
    builder.setRaftGroup(raftGroup);
    RetryLimited retryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(20,
        TimeDuration.ONE_SECOND);
    builder.setRetryPolicy(retryPolicy);
    builder.setClientRpc(new GrpcFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), raftProperties));
    client = builder.build();
    SessionRequest sessionRequest = SessionRequest.of(ci.getUser(), Base58.encode(UUID.randomUUID()));
    UpdateRequest updateRequest = new UpdateRequest()
        .setDb(ci.getDb())
        .setSender(Sender.connection)
        .setType(UpdateType.openSession)
        .setSession(sessionRequest.toSessionId())
        .setPassword(rsaHelper.encrypt(ci.getPassword()));
    UpdateReply updateReplyProto = sendUpdate(updateRequest);
    if(updateReplyProto.getEx()!=null){
      throw updateReplyProto.getEx();
    }
    session = sessionRequest.toSessionId();
  }


  QueryReply sendQueryRequest(QueryRequest queryRequest) throws SQLException {
    try {
      WrapRequestProto msgProto = WrapRequestProto.newBuilder()
          .setType(DB)
          .setMsg( fasts.asByteString(queryRequest))
          .build();
      RaftClientReply reply =
          client.io().sendReadOnly(Message.valueOf(msgProto));
      if(reply.getException()==null){
        WrapReplyProto replyProto = WrapReplyProto.parseFrom(reply.getMessage().getContent());
        if(!replyProto.getEx().isEmpty()){
          throw (SQLException) fasts.as(replyProto.getEx());
        }
        return fasts.as(replyProto.getRelay());
      }else {
        throw reply.getException();
      }
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }

  public String getSession() {
    return session;
  }

  public AtomicInteger getUpdateCount() {
    return updateCount;
  }

  public SerialSupport getFasts() {
    return fasts;
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
    if(updateCount.get()>0) {
      UpdateRequest builder = newUpdateRequestBuilder()
          .setType(UpdateType.commit);
      sendUpdate(builder);
      this.tx = UUID.randomUUID().toString();
      updateCount.set(0);
    }
  }


  protected void checkClose() throws SQLException {
    if(isClosed()){
      throw new SQLException("connection is closed.");
    }
  }

  protected UpdateReply sendUpdate(UpdateRequest updateRequest) throws SQLException {
    checkClose();
    try {
      WrapRequestProto msgProto = WrapRequestProto.newBuilder()
          .setType(DB)
          .setMsg(fasts.asByteString(updateRequest))
          .build();
      RaftClientReply reply =
          client.io().send(Message.valueOf(msgProto));
      if(reply.getException()==null) {
        WrapReplyProto replyProto = WrapReplyProto.parseFrom(reply.getMessage().getContent());
        if (!replyProto.getEx().isEmpty()) {
          throw (SQLException) fasts.as(replyProto.getEx());
        }
        UpdateReply updateReply = fasts.as(replyProto.getRelay());
        if (updateReply.getEx() != null) {
          throw updateReply.getEx();
        } else {
          return updateReply;
        }
      }else{
        throw reply.getException();
      }
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }


  @Override
  public void rollback() throws SQLException {
    if(updateCount.get()>0) {
      UpdateRequest builder = newUpdateRequestBuilder()
          .setType(UpdateType.rollback);
      sendUpdate(builder);
      this.tx = UUID.randomUUID().toString();
      updateCount.set(0);
    }
  }

  private UpdateRequest newUpdateRequestBuilder() {
    return new UpdateRequest()
        .setDb(ci.getDb())
        .setSender(Sender.connection)
        .setSession(session)
        .setTx(tx);
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
    try {
      if(session!=null&&!session.isEmpty()) {
        UpdateRequest updateRequest = new UpdateRequest()
            .setDb(ci.getDb())
            .setSender(Sender.connection)
            .setType(UpdateType.closeSession)
            .setSession(session);
        UpdateReply updateReplyProto = sendUpdate(updateRequest);
      }
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
    UpdateRequest builder = newUpdateRequestBuilder()
        .setType(UpdateType.savepoint);
    if(name!=null){
      builder.setSql(name);
    }
    UpdateReply updateReply = sendUpdate(builder);
    return updateReply.getSavepoint();
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    UpdateRequest builder = newUpdateRequestBuilder()
        .setType(UpdateType.rollbackSavepoint)
        .setSavepoint(JdbcSavepoint.of(savepoint));
    sendUpdate(builder);
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    UpdateRequest builder = newUpdateRequestBuilder()
        .setType(UpdateType.releaseSavepoint)
        .setSavepoint(JdbcSavepoint.of(savepoint));
    sendUpdate(builder);
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
