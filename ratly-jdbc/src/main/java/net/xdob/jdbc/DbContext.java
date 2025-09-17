package net.xdob.jdbc;

import net.xdob.ratly.util.Timestamp;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public interface DbContext {
	String getLeaderId();
	String getName();
	String getPeerId();

	ScheduledExecutorService getScheduler();
	SessionMgr getSessionMgr();
	void closeSession(String sessionId);
	Timestamp getLastPauseTime();
}
