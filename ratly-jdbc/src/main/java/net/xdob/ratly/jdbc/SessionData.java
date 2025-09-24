package net.xdob.ratly.jdbc;

public class SessionData {
	private String db;
	private String sessionId;
	private String user;
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


	public long getTx() {
		return tx;
	}

	public SessionData setTx(long tx) {
		this.tx = tx;
		return this;
	}
}
