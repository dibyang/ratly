package net.xdob.jdbc;

import com.google.common.collect.Maps;
import net.xdob.jdbc.sql.SerialResultSetMetaData;
import net.xdob.jdbc.sql.SerialRow;
import net.xdob.jdbc.sql.TransactionIsolation;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.raftlog.RaftLogIndex;
import net.xdob.ratly.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class Session implements AutoCloseable {
	static final Logger LOG = LoggerFactory.getLogger(Session.class);
	public static final int RESULTSET_TIMEOUT = 3_000;
	private final String db;
  private final String user;
  private final String sessionId;
	private final String innerId;

  private final Connection connection;

	private transient volatile Timestamp lastHeartTime = Timestamp.currentTime();
	private final DbContext context;
	private final RaftLogIndex appliedIndex;
  /**
   * 是否处于事务中
   */
  private final AtomicBoolean transaction = new AtomicBoolean(false);
  private State state = State.SLEEP;
	private Long startTime = System.currentTimeMillis();
	private Long sleepSince = System.currentTimeMillis();
	private String sql = "";
	private final Map<String,ResultSetHandler> resultSets = Maps.newConcurrentMap();

  public Session(String db, String user, String sessionId, Connection connection, DbContext context) throws SQLException {
		this.db = db;
		this.user = user;
		this.sessionId = sessionId;
		this.connection = connection;
		try(Statement st = connection.createStatement();
				ResultSet rs = st.executeQuery("select session_id();")){
			if(rs.next()) {
				innerId = rs.getString(1);
			}else {
				throw new SQLException("get session id error");
			}
		}
		this.context = context;
		appliedIndex = new RaftLogIndex(db+"_session_AppliedIndex", RaftLog.INVALID_LOG_INDEX);
	}

	public ResultSet getResultSet(String uuid){
		ResultSetHandler handler = resultSets.get(uuid);
		if(handler!=null){
			handler.updateLastAccessTime();
			return handler.getResultSet();
		}
		return null;
	}

	public void addResultSet(String uuid, ResultSet resultSet){
		ResultSetHandler handler = new ResultSetHandler(resultSet);
		resultSets.put(uuid, handler);
	}

	public void checkExpiredResultSets(){
		for (String uuid : resultSets.keySet()) {
			ResultSetHandler handler = resultSets.get(uuid);
			if(handler.getLastAccessTime().elapsedTimeMs()> RESULTSET_TIMEOUT){
				try {
					closeResultSet(uuid);
				} catch (SQLException e) {
					LOG.warn("close session error", e);
				}
			}
		}
	}

	public void closeResultSet(String uuid) throws SQLException {
		ResultSetHandler removed = resultSets.remove(uuid);
		if(removed!=null){
			removed.close();
		}
	}

	public boolean updateAppliedIndexToMax(long newIndex) {
		return appliedIndex.updateToMax(newIndex,
				message -> LOG.debug("updateAppliedIndex {}", message));
	}

	public State getState() {
		return state;
	}

	public String getSql() {
		return sql;
	}

	public Session running(String sql) {
		this.state = State.RUNNING;
		this.sleepSince = null;
		this.sql = sql;
		return this;
	}

	public Session sleep() {
		this.state = State.SLEEP;
		this.sleepSince = System.currentTimeMillis();
		this.sql = "";
		return this;
	}

	public Long getStartTime() {
		return startTime;
	}


	public Long getSleepSince() {
		return sleepSince;
	}


	public long elapsedHeartTimeMs() {
		if(context.getLastJvmPauseTime().getNanos()>lastHeartTime.getNanos()){
			updateLastHeartTime();
		}
		return lastHeartTime.elapsedTimeMs();
	}

	public void updateLastHeartTime() {
		lastHeartTime = Timestamp.currentTime();
	}

	public Savepoint setSavepoint(String name) throws SQLException {
    checkClosed();
    return connection.setSavepoint(name);
  }

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		checkClosed();
		connection.releaseSavepoint(savepoint);
	}

	public void rollback(Savepoint savepoint) throws SQLException {
		checkClosed();
		connection.rollback(savepoint);
	}

	public Connection getConnection() {
		return connection;
	}

	public boolean isTransaction() {
		return transaction.get();
	}


  private void checkClosed() throws SQLException {
    if(connection.isClosed()){
      throw new SQLException("connection is closed");
    }
  }

  public void setAutoCommit(boolean autoCommit) throws SQLException {
    checkClosed();
		boolean needCommit = !autoCommit && connection.getAutoCommit();
    connection.setAutoCommit(autoCommit);
		if (needCommit) {
			commit(appliedIndex.get());
		}
		checkTransaction();
	}

	/**
	 * 不自动提交，自动开启事务
	 */
	private void checkTransaction() throws SQLException {
		if (!connection.getAutoCommit()) {
			beginTx();
		}
	}

	public void beginTx() {
		transaction.compareAndSet(false, true);
	}

	public boolean getAutoCommit() throws SQLException {
    checkClosed();
    return connection.getAutoCommit();
  }

	public int getTransactionIsolation() throws SQLException {
		checkClosed();
		return connection.getTransactionIsolation();
	}

	public void setTransactionIsolation(int level) throws SQLException {
    checkClosed();
		if(Connection.TRANSACTION_SERIALIZABLE == level) {
			connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
		}else if(Connection.TRANSACTION_REPEATABLE_READ == level) {
			connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
		}else if(Connection.TRANSACTION_READ_COMMITTED == level) {
			connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		}else if(Connection.TRANSACTION_READ_UNCOMMITTED == level) {
			connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
		}else if(Connection.TRANSACTION_NONE == level) {
			connection.setTransactionIsolation(Connection.TRANSACTION_NONE);
		}
  }

	public String getTransactionIsolationName() throws SQLException {
		return TransactionIsolation.of(getTransactionIsolation()).name();
	}


  public void commit(long index) throws SQLException {
    if(transaction.compareAndSet(true, false)){
      connection.commit();
			context.updateAppliedIndexToMax(index);
			//LOG.info("db {} commit index:{}", db, index);
    }
		checkTransaction();
	}
	public void rollback(long index) throws SQLException {
    if(transaction.compareAndSet(true, false)) {
      connection.rollback();
			if(index>=0){
				context.updateAppliedIndexToMax(index);
				//LOG.info("db {} rollback index:{}", db, index);
			}
    }
		checkTransaction();
	}


  public String getDb() {
    return db;
  }

  public String getSessionId() {
    return sessionId;
  }

  public String getUser() {
    return user;
  }


  public void close() throws Exception {
		for (String uid : resultSets.keySet()) {
			closeResultSet(uid);
		}
		resultSets.clear();
    if (!connection.isClosed()) {
			try {
				rollback(-1);
			} catch (SQLException ignore) {
			}
      connection.close();
      LOG.info("session connection close id={}", sessionId);
			context.getSessionMgr().closeSession(this.sessionId);
    }
  }

	public void setSessionData(SessionData sessionData) throws SQLException {
		this.setAutoCommit(sessionData.isAutoCommit());
		this.setTransactionIsolation(sessionData.getTransactionIsolation());
		this.sleep();
	}

	public SessionData toSessionData() {
		try {
			return new SessionData()
					.setDb(this.db)
					.setSessionId(this.sessionId)
					.setUser(this.user)
					.setAutoCommit(this.getAutoCommit())
					.setTransactionIsolation(this.getTransactionIsolation());
		} catch (SQLException e) {
			return null;
		}
	}


	public Map<String, Object> toMap() {
		try {
			Map<String, Object> map = new HashMap<>();
			map.put("db", this.db);
			map.put("sessionId", this.sessionId);
			map.put("innerId", this.innerId);
			map.put("user", this.user);
			map.put("autoCommit", this.getAutoCommit());
			map.put("transactionIsolation", getTransactionIsolationName());
			map.put("state", this.state);
			map.put("lastHeart", this.lastHeartTime.elapsedTimeMs());
			map.put("startTime", new java.sql.Timestamp(this.startTime));
			map.put("sleepSince", this.sleepSince == null ? null : new java.sql.Timestamp(this.sleepSince));
			map.put("sql", this.sql);

			return map;
		} catch (SQLException e) {
			return null;
		}
	}

	public SerialRow toSerialRow() throws SQLException {
		int index = 0;
		return new SerialRow(11)
				.setValue(index++, this.db)
				.setValue(index++, this.sessionId)
				.setValue(index++, this.innerId)
				.setValue(index++, this.user)
				.setValue(index++, this.getAutoCommit())
				.setValue(index++, getTransactionIsolationName())
				.setValue(index++, this.state.name())
				.setValue(index++, this.lastHeartTime.elapsedTimeMs())
				.setValue(index++, new java.sql.Timestamp(this.startTime))
				.setValue(index++, this.sleepSince == null ?
						null : new java.sql.Timestamp(this.sleepSince))
				.setValue(index++, this.sql);

	}

	public static SerialResultSetMetaData buildSessionResultSetMetaData() {
		SerialResultSetMetaData metaData = new SerialResultSetMetaData();
		metaData.addColumn("db", JDBCType.VARCHAR.getVendorTypeNumber(), 0, 0);
		metaData.addColumn("session_id", JDBCType.VARCHAR.getVendorTypeNumber(), 0, 0);
		metaData.addColumn("inner_id", JDBCType.VARCHAR.getVendorTypeNumber(), 0, 0);
		metaData.addColumn("user", JDBCType.VARCHAR.getVendorTypeNumber(), 0, 0);
		metaData.addColumn("auto_commit", JDBCType.BOOLEAN.getVendorTypeNumber(), 0, 0);
		metaData.addColumn("transaction_isolation", JDBCType.VARCHAR.getVendorTypeNumber(), 0, 0);
		metaData.addColumn("state", JDBCType.VARCHAR.getVendorTypeNumber(), 0, 0);
		metaData.addColumn("last_heart", JDBCType.INTEGER.getVendorTypeNumber(), 0, 0);
		metaData.addColumn("start_time", JDBCType.TIMESTAMP.getVendorTypeNumber(), 0, 0);
		metaData.addColumn("sleep_since", JDBCType.TIMESTAMP.getVendorTypeNumber(), 0, 0);
		metaData.addColumn("sql", JDBCType.VARCHAR.getVendorTypeNumber(), 0, 0);

		return metaData;
	}

	public enum State {
		RUNNING,
		SLEEP,
		CLOSED
	}

}
