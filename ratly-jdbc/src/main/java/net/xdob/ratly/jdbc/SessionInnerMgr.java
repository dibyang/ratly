package net.xdob.ratly.jdbc;

public interface SessionInnerMgr extends SessionMgr {
	Session removeSession(String sessionId);
}
