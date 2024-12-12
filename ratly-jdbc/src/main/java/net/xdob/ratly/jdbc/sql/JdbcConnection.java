package net.xdob.ratly.jdbc.sql;

import com.google.protobuf.ByteString;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.fasts.serialization.FSTConfiguration;
import net.xdob.ratly.grpc.GrpcFactory;
import net.xdob.ratly.proto.jdbc.*;
import net.xdob.ratly.protocol.*;
import org.h2.message.DbException;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public class JdbcConnection implements Connection {
  private final String url;
  private final JdbcConnectionInfo ci;
  private final RaftClient client;
  private final FSTConfiguration fasts = FSTConfiguration.createDefaultConfiguration();
  private boolean autoCommit;
  private String tx = UUID.randomUUID().toString();
  private final AtomicInteger updateCount = new AtomicInteger();

  public JdbcConnection(JdbcConnectionInfo ci) {
    this.url = ci.getUrl();
    this.ci = ci;
    RaftProperties raftProperties = new RaftProperties();
    final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(ci.getGroup())),
        ci.getPeers());
    RaftClient.Builder builder =
        RaftClient.newBuilder().setProperties(raftProperties);
    builder.setRaftGroup(raftGroup);
    builder.setClientRpc(new GrpcFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), raftProperties));
    client = builder.build();
  }

  public AtomicInteger getUpdateCount() {
    return updateCount;
  }

  public FSTConfiguration getFasts() {
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
    UpdateRequestProto.Builder builder = UpdateRequestProto.newBuilder()
        .setSender(Sender.connection)
        .setTx(tx)
        .setDb(ci.getDb())
        .setType(UpdateType.commit);
    sendUpdate(builder.build());
    this.tx = UUID.randomUUID().toString();
    updateCount.set(0);
  }

  public SQLException getSQLException(SQLExceptionProto ex) {
    SQLException e = new SQLException(ex.getReason(), ex.getState(), ex.getErrorCode());
    e.setStackTrace((StackTraceElement[]) fasts.asObject(ex.getStacktrace().toByteArray()));
    return e;
  }

  protected void checkClose() throws SQLException {
    if(isClosed()){
      throw new SQLException("connection is closed.");
    }
  }

  protected UpdateReplyProto sendUpdate(UpdateRequestProto updateRequest) throws SQLException {
    checkClose();
    try {
      RaftClientReply reply =
          client.io().send(Message.valueOf(updateRequest));
      UpdateReplyProto updateReply = UpdateReplyProto.parseFrom(reply.getMessage().getContent());
      if(updateReply.hasEx()) {
        throw getSQLException(updateReply.getEx());
      }else{
        return updateReply;
      }
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }


  @Override
  public void rollback() throws SQLException {
    UpdateRequestProto.Builder builder = UpdateRequestProto.newBuilder()
        .setSender(Sender.connection)
        .setTx(tx)
        .setDb(ci.getDb())
        .setType(UpdateType.rollback);
    sendUpdate(builder.build());
    this.tx = UUID.randomUUID().toString();
    updateCount.set(0);
  }

  @Override
  public void close() throws SQLException {
    if(!this.autoCommit&&updateCount.get()>0){
      try {
        rollback();
      }catch (SQLException e){
        // ignore SQLException
      }
    }
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
    UpdateRequestProto.Builder builder = UpdateRequestProto.newBuilder()
        .setSender(Sender.connection)
        .setTx(tx)
        .setDb(ci.getDb())
        .setType(UpdateType.savepoint);
    if(name!=null){
      builder.setSql(name);
    }
    UpdateReplyProto updateReplyProto = sendUpdate(builder.build());
    SavepointProto savepoint = updateReplyProto.getSavepoint();
    return new JdbcSavepoint(savepoint.getId(), savepoint.getName());
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    UpdateRequestProto.Builder builder = UpdateRequestProto.newBuilder()
        .setSender(Sender.connection)
        .setTx(tx)
        .setDb(ci.getDb())
        .setType(UpdateType.rollbackSavepoint)
        .setSavepoint(SavepointProto.newBuilder()
            .setId(savepoint.getSavepointId())
            .setName(savepoint.getSavepointName())
            .build());
    sendUpdate(builder.build());
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    UpdateRequestProto.Builder builder = UpdateRequestProto.newBuilder()
        .setSender(Sender.connection)
        .setTx(tx)
        .setDb(ci.getDb())
        .setType(UpdateType.releaseSavepoint)
        .setSavepoint(SavepointProto.newBuilder()
            .setId(savepoint.getSavepointId())
            .setName(savepoint.getSavepointName())
            .build());
    sendUpdate(builder.build());
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    return null;
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
    return false;
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {

  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {

  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    return "";
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
