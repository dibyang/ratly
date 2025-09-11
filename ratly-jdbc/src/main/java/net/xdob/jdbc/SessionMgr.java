package net.xdob.jdbc;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public interface SessionMgr {
  /**
   * 新建托管的session
   */
  Session newSession(String db, String user, String sessionId, ConnSupplier connSupplier) throws SQLException;


  Optional<Session> getSession(String id);
  List<Session> getAllSessions();
  boolean closeSession(String sessionId);
  void clearSessions();
  void checkExpiredSessions();
	boolean isTransaction();
	void setDisabled(boolean disabled);
}
