package net.xdob.ratly.jdbc;

import com.google.common.collect.Maps;
import net.xdob.onlooker.security.Base58;
import net.xdob.ratly.util.Finder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class DefaultSessionMgr implements SessionMgr{
  static final Logger LOG = LoggerFactory.getLogger(DefaultSessionMgr.class);

  public static final int TIME_OUT = 5 * 60;
  private final ConcurrentMap<String, Session> sessions = Maps.newConcurrentMap();

  @Override
  public Session newSession(String user, ConnSupplier connSupplier) throws SQLException {
    Session session = new Session(user + "$" + Base58.encodeUuid(UUID.randomUUID()), user, connSupplier, this::removeSession);
    sessions.put(session.getId(), session);
    return session;
  }

  public Session getOrOpenSession(String sessionId, ConnSupplier connSupplier) throws SQLException {
    synchronized (sessions) {
      Session session = getSession(sessionId).orElse(null);
      if(session==null) {
        String user = Finder.c(sessionId).head("$").getValue();
        session = new Session(sessionId, user, connSupplier, this::removeSession);
        sessions.put(session.getId(), session);
        //LOG.info("open session={}", sessionId);
      }
      return session;
    }
  }

  @Override
  public Optional<Session> getSession(String sessionId) {
    return Optional.ofNullable(sessionId==null?null:sessions.get(sessionId));
  }

  void removeSession(String sessionId) {
    if (sessionId != null) {
      sessions.remove(sessionId);
      LOG.info("remove session={}", sessionId);
    }
  }

  @Override
  public void checkTimeout() {
    synchronized (sessions){
      List<Session> timeoutSessions = sessions.values().stream().filter(e -> e.getAccessTimeOffset() > TIME_OUT)
          .collect(Collectors.toList());
      for (Session session : timeoutSessions) {
        sessions.remove(session.getId());
        try {
          session.close();
        } catch (Exception e) {
          LOG.warn("", e);
        }
      }
    }
  }
}
