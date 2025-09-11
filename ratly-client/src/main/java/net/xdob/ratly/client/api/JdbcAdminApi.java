package net.xdob.ratly.client.api;


import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface JdbcAdminApi {
  List<Map<String, Object>> showSession() throws SQLException;
  boolean killSession(String sessionId) throws SQLException;
}
