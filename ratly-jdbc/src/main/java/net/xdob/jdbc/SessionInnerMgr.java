package net.xdob.jdbc;

public interface SessionInnerMgr extends SessionMgr {
	Session removeSession(String sessionId);
}
