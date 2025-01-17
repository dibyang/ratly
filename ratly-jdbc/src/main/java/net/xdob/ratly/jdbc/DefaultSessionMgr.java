package net.xdob.ratly.jdbc;

import com.google.common.collect.Maps;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

public class DefaultSessionMgr implements SessionMgr{
  private final ConcurrentMap<String, Session> sessions = Maps.newConcurrentMap();

  @Override
  public Session newSession(String user, String password) {
    Session session = new Session(UUID.randomUUID().toString(), user);
    sessions.put(session.getId(), session);
    return session;
  }

  @Override
  public Optional<Session> getSession(String id) {
    return Optional.ofNullable(id==null?null:sessions.get(id));
  }

  @Override
  public Optional<Session> removeSession(String id) {
    return Optional.ofNullable(id==null?null:sessions.remove(id));
  }
}
