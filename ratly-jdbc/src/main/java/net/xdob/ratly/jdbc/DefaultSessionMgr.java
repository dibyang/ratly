package net.xdob.ratly.jdbc;

import com.google.common.collect.Maps;
import net.xdob.ratly.jdbc.exception.SessionIdAlreadyExistsException;
import net.xdob.ratly.security.Base58;
import net.xdob.ratly.util.Finder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class DefaultSessionMgr implements SessionMgr{
  static final Logger LOG = LoggerFactory.getLogger(DefaultSessionMgr.class);

  public static final int TIME_OUT = 5 * 60;
  private final ConcurrentMap<String, Session> sessions = Maps.newConcurrentMap();

  @Override
  public Session newSession(SessionRequest sessionRequest, ConnSupplier connSupplier) throws SQLException {
    synchronized (sessions) {
      String sessionId = sessionRequest.toSessionId();
      Session session = getSession(sessionId).orElse(null);
      if (session != null) {
        throw new SessionIdAlreadyExistsException(sessionId);
      }
      session = new Session(sessionRequest, connSupplier, this::removeSession);
      sessions.put(session.getId(), session);
      return session;
    }
  }

  @Override
  public Session getOrOpenSession(String sessionId, ConnSupplier connSupplier) throws SQLException {
    synchronized (sessions) {
      Session session = getSession(sessionId).orElse(null);
      if(session==null) {
        SessionRequest request = SessionRequest.fromSessionId(sessionId);
        session = new Session(request, connSupplier, this::removeSession);
        session.updateAccessTime();
        sessions.put(session.getId(), session);
        //LOG.info("open session={}", sessionId);
      }
      return session;
    }
  }

  @Override
  public Optional<Session> getSession(String sessionId) {
    Optional<Session> session = Optional.ofNullable(sessionId == null ? null : sessions.get(sessionId));
    session.ifPresent(Session::updateAccessTime);
    return session;
  }

  @Override
  public List<Session> getAllSessions() {
    return new ArrayList<>(sessions.values());
  }

  @Override
  public void removeSession(String sessionId) {
    if (sessionId != null) {
      sessions.remove(sessionId);
      //LOG.info("remove session={}", sessionId);
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
