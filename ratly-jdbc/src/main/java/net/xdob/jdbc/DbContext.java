package net.xdob.jdbc;

import java.util.concurrent.ScheduledExecutorService;

public interface DbContext {
	String getLeaderId();
	String getName();
	String getPeerId();
	/**
	 * 事务完成更新插件事务阶段性索引
	 * @param newIndex 插件事务阶段性索引
	 */
	boolean updateAppliedIndexToMax(long newIndex);
	ScheduledExecutorService getScheduler();
	SessionMgr getSessionMgr();
	void closeSession(String sessionId);
}
