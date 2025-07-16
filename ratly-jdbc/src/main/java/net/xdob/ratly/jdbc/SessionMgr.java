package net.xdob.ratly.jdbc;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public interface SessionMgr {
  /**
   * 新建托管的session
   */
  Session newSession(SessionRequest sessionRequest, ConnSupplier connSupplier) throws SQLException;


  Optional<Session> getSession(String id);
  List<Session> getAllSessions();
  Session removeSession(String sessionId);
  void closeSession(String sessionId);
  void checkTimeout();
}
