package net.xdob.jdbc;

import net.xdob.jdbc.sql.SerialResultSetMetaData;
import net.xdob.jdbc.sql.SerialRow;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.raftlog.RaftLogIndex;
import net.xdob.ratly.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class Session implements AutoCloseable {
  static final Logger LOG = LoggerFactory.getLogger(Session.class);
  private final String db;
  private final String user;
  private final String sessionId;

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
	private final Map<String,ResultSet> resultSets = new HashMap<>();

  public Session(String db, String user, String sessionId, Connection connection, DbContext context) {
		this.db = db;
		this.user = user;
		this.sessionId = sessionId;
		this.connection = connection;
		this.context = context;
		appliedIndex = new RaftLogIndex(db+"_session_AppliedIndex", RaftLog.INVALID_LOG_INDEX);
	}

	public ResultSet getResultSet(String uuid){
		return resultSets.get(uuid);
	}

	public void addResultSet(String uuid, ResultSet resultSet){
		resultSets.put(uuid, resultSet);
	}

	public void closeResultSet(String uuid) throws SQLException {
		ResultSet removed = resultSets.remove(uuid);
		if(removed!=null){
			removed.getStatement().close();
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

	private String getTransactionIsolationName(int level){
		switch (level) {
			case Connection.TRANSACTION_SERIALIZABLE:
				return "TRANSACTION_SERIALIZABLE";
			case Connection.TRANSACTION_REPEATABLE_READ:
				return "TRANSACTION_REPEATABLE_READ";
			case Connection.TRANSACTION_READ_COMMITTED:
				return "TRANSACTION_READ_COMMITTED";
			case Connection.TRANSACTION_READ_UNCOMMITTED:
				return "TRANSACTION_READ_UNCOMMITTED";
		}
		return "TRANSACTION_NONE";
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
		for (ResultSet resultSet : resultSets.values()) {
			resultSet.getStatement().close();
			resultSet.close();
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
			map.put("sessionId", this.sessionId);
			map.put("user", this.user);
			map.put("autoCommit", this.getAutoCommit());
			map.put("transactionIsolation", this.getTransactionIsolation());
			map.put("state", this.state);
			map.put("sql", this.sql);
			map.put("startTime", new Date(this.startTime).toInstant());
			map.put("sleepSince", this.sleepSince == null ? null : new Date(this.sleepSince).toInstant());
			return map;
		} catch (SQLException e) {
			return null;
		}
	}

	public SerialRow toSerialRow() throws SQLException {
		return new SerialRow(10)
				.setValue(0, this.db)
				.setValue(1, this.sessionId)
				.setValue(2, this.user)
				.setValue(3, this.getAutoCommit())
				.setValue(4, getTransactionIsolationName(this.getTransactionIsolation()))
				.setValue(5, this.state.name())
				.setValue(6, this.lastHeartTime.elapsedTimeMs())
				.setValue(7, new java.sql.Timestamp(this.startTime))
				.setValue(8, this.sleepSince == null ?
						null : new java.sql.Timestamp(this.sleepSince))
				.setValue(9, this.sql);

	}

	public static SerialResultSetMetaData buildSessionResultSetMetaData() {
		SerialResultSetMetaData metaData = new SerialResultSetMetaData();
		metaData.addColumn("db", JDBCType.VARCHAR.getVendorTypeNumber(), 0, 0);
		metaData.addColumn("session_id", JDBCType.VARCHAR.getVendorTypeNumber(), 0, 0);
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
