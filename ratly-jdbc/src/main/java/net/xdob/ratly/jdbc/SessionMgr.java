package net.xdob.ratly.jdbc;

import net.xdob.ratly.util.MemoizedCheckedSupplier;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public interface SessionMgr extends AutoCloseable {

	/**
	 * 获取正在使用的session数量(活动的)
	 */
	int getActiveCount();
	/**
	 * 获取可用的session数量
	 */
	int getAvailableSessionCount();
  /**
   * 新建托管的session
   */
  Session newSession(String db, String user, String sessionId, MemoizedCheckedSupplier<Connection, SQLException> connSupplier) throws SQLException;


  Optional<Session> getSession(String id);
  List<Session> getAllSessions();
  boolean closeSession(String sessionId, long index);
  void clearSessions();
  void checkExpiredSessions();
	boolean isTransaction();
	void setDisabled(boolean disabled);
	/**
	 * 获取插件已结束事务的索引列表
	 */
	List<Long> getLastEndedTxIndexList();
	/**
	 * 获取插件内最早事务的开始索引
	 */
	long getFirstTx();
}
