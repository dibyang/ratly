package net.xdob.ratly.jdbc.sql;

import net.xdob.ratly.jdbc.exception.SessionAlreadyClosedException;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.proto.jdbc.*;
import net.xdob.ratly.proto.sm.WrapReplyProto;
import net.xdob.ratly.proto.sm.WrapRequestProto;
import net.xdob.ratly.protocol.*;
import net.xdob.ratly.retry.RetryPolicies;
import net.xdob.ratly.retry.RetryPolicy;
import net.xdob.ratly.security.RsaHelper;
import net.xdob.ratly.util.Proto2Util;
import net.xdob.ratly.util.TimeDuration;
import org.h2.message.DbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class JdbcConnection implements Connection {
  static final Logger LOG = LoggerFactory.getLogger(JdbcConnection.class);
  public static final String DB = "db";

  private final String url;
  private final JdbcConnectionInfo ci;
  private final RaftClient client;

  private final Properties properties = new Properties();
  private final RsaHelper rsaHelper = new RsaHelper();
  private final AtomicReference<String> sessionId = new AtomicReference<>();
	private final ScheduledExecutorService scheduledService;

	public JdbcConnection(JdbcConnectionInfo ci) throws SQLException {
		this.url = ci.getUrl();
    this.ci = ci;
		scheduledService = Executors.newScheduledThreadPool(1);
    RaftProperties raftProperties = new RaftProperties();
    final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ci.getGroup()),
        ci.getPeers());
    RaftClient.Builder builder =
        RaftClient.newBuilder().setProperties(raftProperties);
    builder.setRaftGroup(raftGroup);
    RetryPolicy retryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(8,
        TimeDuration.valueOf(20, TimeUnit.MILLISECONDS));
    builder.setRetryPolicy(retryPolicy);
		builder.setParameters(new Parameters());
		try {
			client = builder.build();
			open();
		} catch (SQLException e) {
			close();
			throw e;
		}

		scheduledService.scheduleWithFixedDelay(this::sendHeartBeat, 200, 500, TimeUnit.MILLISECONDS);
	}

	private void open() throws SQLException {
		sessionId.set(openSession());
		LOG.info("open session: {}", sessionId.get());
	}

	private void sendHeartBeat() {
		String session = getSession();
		if(session !=null){
			JdbcRequestProto requestProto = JdbcRequestProto.newBuilder()
					.setDb(ci.getDb())
					.setHeartbeat(HeartbeatProto.newBuilder())
					.setSessionId(session)
					.build();
			try {
				this.checkClose();
				WrapRequestProto wrap = WrapRequestProto.newBuilder()
						.setType(JdbcConnection.DB)
						.setJdbcRequest(requestProto)
						.build();
				RaftClientReply reply = client.io().sendAdmin(Message.valueOf(wrap));
				WrapReplyProto replyProto = WrapReplyProto.parseFrom(reply.getMessage().getContent());
				JdbcResponseProto response = replyProto.getJdbcResponse();
				if(response.hasEx()){
					throw Proto2Util.toSQLException(response.getEx());
				}
			} catch (Exception e) {
				LOG.warn("send heartbeat error", e);
				if((e instanceof SessionAlreadyClosedException)
				||(e instanceof IOException)) {
					try {
						close();
					} catch (SQLException ignored) {

					}
				}
			}
		}
	}


  private String openSession() throws SQLException {
		OpenSessionProto.Builder openSession = OpenSessionProto.newBuilder()
				.setUser(ci.getUser())
				.setPassword(rsaHelper.encrypt(ci.getPassword()));
		JdbcRequestProto request = JdbcRequestProto.newBuilder()
				.setDb(ci.getDb())
				.setPreSession(PreSessionProto.newBuilder())
				.setOpenSession(openSession)
				.build();
		JdbcResponseProto response = sendAdmin(request);
		JdbcRequestProto requestProto = JdbcRequestProto.newBuilder()
				.setDb(ci.getDb())
				.setConnRequest(newConnRequestBuilder(ConnRequestType.openSession))
				.setOpenSession(openSession)
				.build();
    JdbcResponseProto responseProto = send(requestProto);
    return responseProto.getSessionId();
  }


  public String getSession() {
    return sessionId.get();
  }

  @Override
  public Statement createStatement() throws SQLException {
		checkClose();
    return new JdbcStatement(getSqlClient());
  }

  private SqlClient getSqlClient() {
    return new SqlClient(client, this, ci);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
		checkClose();
		return new JdbcPreparedStatement(getSqlClient(), sql);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw DbException.getUnsupportedException("CallableStatement");
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
		checkClose();
		ConnRequestProto.Builder connReqBuilder = newConnRequestBuilder(ConnRequestType.nativeSQL)
				.setSql( sql);
		JdbcRequestProto requestProto = newJdbcRequestBuilder()
				.setConnRequest(connReqBuilder.build())
				.build();
		JdbcResponseProto responseProto = sendReadOnly(requestProto);
		return responseProto.getConnection().getSql();
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
		checkClose();
		if(getAutoCommit()!=autoCommit) {
			ConnRequestProto.Builder connReqBuilder = newConnRequestBuilder(ConnRequestType.setAutoCommit)
					.setAutoCommit(autoCommit);
			JdbcRequestProto requestProto = newJdbcRequestBuilder()
					.setConnRequest(connReqBuilder.build())
					.build();
			send(requestProto);
		}
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
		checkClose();
		ConnRequestProto.Builder connReqBuilder = newConnRequestBuilder(ConnRequestType.getAutoCommit);
		JdbcRequestProto requestProto = newJdbcRequestBuilder()
				.setConnRequest(connReqBuilder.build())
				.build();
		JdbcResponseProto responseProto = sendReadOnly(requestProto);
		return responseProto.getConnection().getAutoCommit();
  }

  @Override
  public void commit() throws SQLException {
    this.checkClose();
		ConnRequestProto.Builder connReqBuilder = newConnRequestBuilder(ConnRequestType.commit);
		JdbcRequestProto requestProto = newJdbcRequestBuilder()
				.setConnRequest(connReqBuilder.build())
				.build();
		send(requestProto);
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
    return builder;
  }

	protected JdbcResponseProto sendAdmin(JdbcRequestProto request) throws SQLException {
		try {
			WrapRequestProto wrap = WrapRequestProto.newBuilder()
					.setType(JdbcConnection.DB)
					.setJdbcRequest(request)
					.build();
			RaftClientReply reply = client.io().sendAdmin(Message.valueOf(wrap));
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

	protected JdbcResponseProto send(JdbcRequestProto request) throws SQLException{
		return send(request, false);
	}

  protected JdbcResponseProto send(JdbcRequestProto request, boolean readOnly) throws SQLException {
    try {
      WrapRequestProto wrap = WrapRequestProto.newBuilder()
          .setType(JdbcConnection.DB)
          .setJdbcRequest(request)
          .build();
      RaftClientReply reply = readOnly ?
          client.io().sendReadOnly(Message.valueOf(wrap)) :
          client.io().send(Message.valueOf(wrap));
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

	protected JdbcResponseProto sendReadOnly(JdbcRequestProto request) throws SQLException {
		return send(request, true);
	}


  @Override
  public void rollback() throws SQLException {
    this.checkClose();
		ConnRequestProto.Builder connReqBuilder = newConnRequestBuilder(ConnRequestType.rollback);
		JdbcRequestProto requestProto = newJdbcRequestBuilder()
				.setConnRequest(connReqBuilder.build())
				.build();
		send(requestProto);
  }


  @Override
  public void close() throws SQLException {
		if(scheduledService!=null){
			scheduledService.shutdown();
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
    return client.isClosed()||sessionId.get()==null;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
		checkClose();
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
		checkClose();
		if(getTransactionIsolation()!=level) {
			ConnRequestProto.Builder connReqBuilder = newConnRequestBuilder(ConnRequestType.setTransactionIsolation)
					.setTransactionIsolation(level);
			JdbcRequestProto requestProto = newJdbcRequestBuilder()
					.setConnRequest(connReqBuilder.build())
					.build();
			send(requestProto);
		}
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
		checkClose();
		ConnRequestProto.Builder connReqBuilder = newConnRequestBuilder(ConnRequestType.getTransactionIsolation);
		JdbcRequestProto requestProto = newJdbcRequestBuilder()
				.setConnRequest(connReqBuilder.build())
				.build();
		JdbcResponseProto responseProto = sendReadOnly(requestProto);
		return responseProto.getConnection().getTransactionIsolation();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
		checkClose();
		ConnRequestProto.Builder connReqBuilder = newConnRequestBuilder(ConnRequestType.getWarnings);
		JdbcRequestProto requestProto = newJdbcRequestBuilder()
				.setConnRequest(connReqBuilder.build())
				.build();
		JdbcResponseProto responseProto = sendReadOnly(requestProto);
		ConnectionResponse connection = responseProto.getConnection();
		if(connection.hasSQLWarning()) {
			SQLException sqlEx = Proto2Util.toSQLException(connection.getSQLWarning());
			return new SQLWarning(sqlEx.getMessage(), sqlEx.getSQLState(), sqlEx.getErrorCode());
		}else{
			return null;
		}
	}

  @Override
  public void clearWarnings() throws SQLException {
		checkClose();
		ConnRequestProto.Builder connReqBuilder = newConnRequestBuilder(ConnRequestType.clearWarnings);
		JdbcRequestProto requestProto = newJdbcRequestBuilder()
				.setConnRequest(connReqBuilder.build())
				.build();
		sendReadOnly(requestProto);
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
    return properties;
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
