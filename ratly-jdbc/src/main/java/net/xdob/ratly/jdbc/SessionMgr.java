package net.xdob.ratly.jdbc;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public interface SessionMgr {
  /**
   * 新建托管的session
   */
  Session newSession(SessionRequest sessionRequest, ConnSupplier connSupplier) throws SQLException;

  /**
   * 打开非托管的session
   */
  Session getOrOpenSession(String sessionId, ConnSupplier connSupplier) throws SQLException;
  Optional<Session> getSession(String id);
  List<Session> getAllSessions();
  void removeSession(String sessionId);
  void checkTimeout();
}
