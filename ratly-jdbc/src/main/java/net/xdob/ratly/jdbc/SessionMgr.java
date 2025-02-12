package net.xdob.ratly.jdbc;

import java.sql.SQLException;
import java.util.Optional;

public interface SessionMgr {
  /**
   * 新建托管的session
   */
  Session newSession(String user, ConnSupplier connSupplier) throws SQLException;

  /**
   * 打开非托管的session
   */
  Session getOrOpenSession(String sessionId, ConnSupplier connSupplier) throws SQLException;
  Optional<Session> getSession(String id);
  void checkTimeout();
}
