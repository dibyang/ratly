package net.xdob.ratly.jdbc;

import net.xdob.ratly.jdbc.sql.SerialResultSetMetaData;
import net.xdob.ratly.jdbc.sql.SerialRow;
import net.xdob.ratly.jdbc.sql.TransactionIsolation;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.raftlog.RaftLogIndex;
import net.xdob.ratly.util.MemoizedCheckedSupplier;
import net.xdob.ratly.util.function.CheckedConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class Session  {
	static final Logger LOG = LoggerFactory.getLogger(Session.class);
	/**
	 * 生命周期计数时间(单位是毫秒)
	 */
	public static final int LIFE_TIME = 100;
	private final String db;
  private final String user;
  private final String sessionId;

  private final MemoizedCheckedSupplier<Connection, SQLException> connSupplier;

	private final AtomicLong sinceLastHeartbeat = new AtomicLong(0);
	private final AtomicReference<SessionInnerMgr> sessionMgrRef = new AtomicReference<>();
	private final RaftLogIndex appliedIndex;
  /**
   * 是否处于事务中
   */
  private final AtomicBoolean transaction = new AtomicBoolean(false);
	private final AtomicLong tx = new AtomicLong(0);
  private State state = State.SLEEP;
	private Long startTime = System.currentTimeMillis();
	private Long sleepSince = System.currentTimeMillis();
	private String sql = "";
	private boolean autoCommit = true;
	private int transactionIsolation = Connection.TRANSACTION_READ_COMMITTED;

  public Session(String db, String user, String sessionId, MemoizedCheckedSupplier<Connection, SQLException> connSupplier, SessionInnerMgr sessionMgr) throws SQLException {
		this.db = db;
		this.user = user;
		this.sessionId = sessionId;
		this.connSupplier = connSupplier;
		this.sessionMgrRef.set(sessionMgr);

		appliedIndex = new RaftLogIndex(db+"_session_AppliedIndex", RaftLog.INVALID_LOG_INDEX);
		autoCommit = true;
		transactionIsolation = Connection.TRANSACTION_READ_COMMITTED;
	}

	Connection getConnection() throws SQLException {
		return connSupplier.get();
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
		return sinceLastHeartbeat.addAndGet(LIFE_TIME);
	}

	public void heartBeat() {
		sinceLastHeartbeat.set(0);
	}

	public long getElapsedHeartTimeMs() {
		return sinceLastHeartbeat.get();
	}

	public Savepoint setSavepoint(String name) throws SQLException {
		return getConnection().setSavepoint(name);
  }

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		getConnection().releaseSavepoint(savepoint);
	}

	public void rollback(Savepoint savepoint) throws SQLException {
		getConnection().rollback(savepoint);
	}



	public boolean isTransaction() {
		return transaction.get();
	}


  public void setAutoCommit(boolean autoCommit) throws SQLException {

		this.autoCommit = autoCommit;
		Connection connection = getConnection();
		boolean needCommit = !connection.getAutoCommit() && autoCommit;
    connection.setAutoCommit(autoCommit);
		if (needCommit) {
			commit(appliedIndex.get());
		}
		if (!autoCommit) {
			beginTx();
		}
	}



	public void beginTx() {
		if(transaction.compareAndSet(false, true)){
			tx.set(0);
		}
	}

	public boolean getAutoCommit() throws SQLException {
    return autoCommit;
  }

	public int getTransactionIsolation() throws SQLException {
		return transactionIsolation;
	}

	public void setTransactionIsolation(int level) throws SQLException {
		if(Connection.TRANSACTION_SERIALIZABLE == level) {
			transactionIsolation = Connection.TRANSACTION_SERIALIZABLE;
		}else if(Connection.TRANSACTION_REPEATABLE_READ == level) {
			transactionIsolation = Connection.TRANSACTION_REPEATABLE_READ;
		}else if(Connection.TRANSACTION_READ_COMMITTED == level) {
			transactionIsolation = Connection.TRANSACTION_READ_COMMITTED;
		}else if(Connection.TRANSACTION_READ_UNCOMMITTED == level) {
			transactionIsolation = Connection.TRANSACTION_READ_UNCOMMITTED;
		}else if(Connection.TRANSACTION_NONE == level) {
			transactionIsolation = Connection.TRANSACTION_NONE;
		}
		Connection connection = getConnection();
		connection.setTransactionIsolation(transactionIsolation);
  }

	public String getTransactionIsolationName() throws SQLException {
		return TransactionIsolation.of(getTransactionIsolation()).name();
	}

	public void updateTx(long index){
		this.tx.compareAndSet(0, index);
	}

	public boolean isActive() {
		return connSupplier.isInitialized();
	}

	public void inactive(){
		connSupplier.release();
	}

  public void commit(long index) throws SQLException {
		endTx(index,  Connection::commit);
	}

	private void endTx(long index, CheckedConsumer<Connection,SQLException> consumer) throws SQLException {
		Connection connection = getConnection();
		if(transaction.compareAndSet(true, false)){
			consumer.accept(connection);
			updateAppliedIndexToMax(index);
			tx.set(0);
		}
		if (!connection.getAutoCommit()) {
			beginTx();
		}
	}

	public void rollback(long index) throws SQLException {
		endTx(index, Connection::rollback);
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

	public long getTx(){
		return tx.get();
	}

	public long getAppliedIndex() {
		return appliedIndex.get();
	}


  public void close() throws Exception {
    if (connSupplier.isInitialized() && !connSupplier.get().isClosed()) {
			connSupplier.get().close();
			connSupplier.release();
      LOG.info("session connection close id={}", sessionId);
    }
		sessionMgrRef.updateAndGet(m -> {
			if(m!=null){
				m.removeSession(sessionId);
			}
			return null;
		});
  }

	public void setSessionData(SessionData sessionData) throws SQLException {
		this.tx.set(sessionData.getTx());
		this.sleep();
	}

	public SessionData toSessionData() {
		return new SessionData()
				.setDb(this.db)
				.setSessionId(this.sessionId)
				.setTx(this.tx.get())
				.setUser(this.user);
	}


	public Map<String, Object> toMap() {
		try {
			Map<String, Object> map = new HashMap<>();
			map.put("db", this.db);
			map.put("sessionId", this.sessionId);
			map.put("user", this.user);
			map.put("active", this.isActive());
			map.put("autoCommit", this.getAutoCommit());
			map.put("tx", this.getTx());
			map.put("transactionIsolation", getTransactionIsolationName());
			map.put("state", this.state);
			map.put("lastHeart", this.getElapsedHeartTimeMs());
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
				.setValue(index++, this.user)
				.setValue(index++, this.isActive())
				.setValue(index++, this.getAutoCommit())
				.setValue(index++, this.getTx())
				.setValue(index++, getTransactionIsolationName())
				.setValue(index++, this.state.name())
				.setValue(index++, this.getElapsedHeartTimeMs())
				.setValue(index++, new java.sql.Timestamp(this.startTime))
				.setValue(index++, this.sleepSince == null ?
						null : new java.sql.Timestamp(this.sleepSince))
				.setValue(index++, this.sql);

	}

	public static SerialResultSetMetaData buildSessionResultSetMetaData() {
		SerialResultSetMetaData metaData = new SerialResultSetMetaData();
		metaData.addColumn("db", JDBCType.VARCHAR.getVendorTypeNumber(), 0, 0);
		metaData.addColumn("session_id", JDBCType.VARCHAR.getVendorTypeNumber(), 0, 0);
		metaData.addColumn("user", JDBCType.VARCHAR.getVendorTypeNumber(), 0, 0);
		metaData.addColumn("active", JDBCType.BOOLEAN.getVendorTypeNumber(), 0, 0);
		metaData.addColumn("auto_commit", JDBCType.BOOLEAN.getVendorTypeNumber(), 0, 0);
		metaData.addColumn("tx", JDBCType.INTEGER.getVendorTypeNumber(), 0, 0);
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
