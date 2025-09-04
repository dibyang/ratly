package net.xdob.ratly.client.api;


import java.util.List;
import java.util.Map;

public interface JdbcAdminApi {
  List<Map<String, Object>> showSession();
  boolean killSession(String sessionId);
}
