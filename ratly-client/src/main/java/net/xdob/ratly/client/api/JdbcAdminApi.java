package net.xdob.ratly.client.api;


import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface JdbcAdminApi {
  List<Map<String, Object>> showSession() throws SQLException;
	List<String> showDatabases() throws SQLException;
  boolean killSession(String sessionId) throws SQLException;
	boolean createDatabase(String db, String user, String password) throws SQLException;
	boolean dropDatabase(String db) throws SQLException;
}
