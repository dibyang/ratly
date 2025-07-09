package net.xdob.ratly.jdbc;

import net.xdob.ratly.util.Finder;


public class SessionRequest {
	private String uid;
	private String user;

	public String getUid() {
		return uid;
	}

	public String getUser() {
		return user;
	}

	public SessionRequest setUid(String uid) {
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

	public static SessionRequest of(String user, String uid){
		return new SessionRequest().setUser(user).setUid(uid);
	}

	public static SessionRequest fromSessionId(String sessionId){
		Finder finder = Finder.c(sessionId);
		String user = finder.head("$").getValue();
		String uid = finder.tail("$").getValue();
		SessionRequest req = new SessionRequest();
		req.setUid(uid);
		req.setUser(user);
		return req;
	}
}
