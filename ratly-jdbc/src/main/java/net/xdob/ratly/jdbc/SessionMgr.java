package net.xdob.ratly.jdbc;

import java.util.Optional;

public interface SessionMgr {
  Session newSession(String user, String password);
  Optional<Session> getSession(String id);
  Optional<Session> removeSession(String id);
}
