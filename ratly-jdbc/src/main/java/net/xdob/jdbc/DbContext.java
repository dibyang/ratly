package net.xdob.jdbc;

import java.util.concurrent.ScheduledExecutorService;

public interface DbContext {
	String getLeaderId();
	String getName();
	String getPeerId();
	ScheduledExecutorService getScheduler();
	void closeSession(String sessionId);
}
