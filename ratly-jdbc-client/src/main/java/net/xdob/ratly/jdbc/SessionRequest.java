package net.xdob.ratly.jdbc;

import net.xdob.ratly.util.Finder;


public class SessionRequest {
	private long uid;
	private String user;
	private String db;

	public String getDb() {
		return db;
	}

	public SessionRequest setDb(String db) {
		this.db = db;
		return this;
	}

	public long getUid() {
		return uid;
	}

	public String getUser() {
		return user;
	}

	public SessionRequest setUid(long uid) {
		this.uid = uid;
		return this;
	}

	public SessionRequest setUser(String user) {
		this.user = user;
		return this;
	}

	public String toSessionId(){
		return user + "$" + uid;
	}

	public static SessionRequest of(String db, String user, long uid){
		return new SessionRequest().setDb(db).setUser(user).setUid(uid);
	}

	public static SessionRequest fromSessionId(String db, String sessionId){
		Finder finder = Finder.c(sessionId);
		String user = finder.head("$").getValue();
		long uid = finder.tail("$").getValue(Long.class);
		SessionRequest req = new SessionRequest();
		req.setUid(uid);
		req.setUser(user);
		req.setDb(db);
		return req;
	}
}
