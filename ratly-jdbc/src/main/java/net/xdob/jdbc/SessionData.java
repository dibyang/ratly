package net.xdob.jdbc;

public class SessionData {
	private String db;
	private String sessionId;
	private String user;
	private boolean autoCommit;
	private int transactionIsolation;
	private long tx;

	public String getDb() {
		return db;
	}

	public SessionData setDb(String db) {
		this.db = db;
		return this;
	}

	public String getSessionId() {
		return sessionId;
	}

	public SessionData setSessionId(String sessionId) {
		this.sessionId = sessionId;
		return this;
	}

	public String getUser() {
		return user;
	}

	public SessionData setUser(String user) {
		this.user = user;
		return this;
	}

	public boolean isAutoCommit() {
		return autoCommit;
	}

	public SessionData setAutoCommit(boolean autoCommit) {
		this.autoCommit = autoCommit;
		return this;
	}

	public int getTransactionIsolation() {
		return transactionIsolation;
	}

	public SessionData setTransactionIsolation(int transactionIsolation) {
		this.transactionIsolation = transactionIsolation;
		return this;
	}

	public long getTx() {
		return tx;
	}

	public SessionData setTx(long tx) {
		this.tx = tx;
		return this;
	}
}
