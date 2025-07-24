package net.xdob.ratly.jdbc;

import com.google.common.collect.Maps;
import net.xdob.ratly.jdbc.exception.SessionIdAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class DefaultSessionMgr implements SessionMgr{
  static final Logger LOG = LoggerFactory.getLogger(DefaultSessionMgr.class);

  private int maxSessions = 32;
  private final ConcurrentMap<String, Session> sessions = Maps.newConcurrentMap();
  private final DbsContext context;

	public DefaultSessionMgr(DbsContext context) {
		this.context = context;
	}

  public int getMaxSessions() {
    return maxSessions;
  }

  public DefaultSessionMgr setMaxSessions(int maxSessions) {
    this.maxSessions = maxSessions;
    return this;
  }

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
      LOG.info("node {} new session={}", context.getPeerId(), sessionId);
      checkExpiredSessions();
      return session;
    }
  }

  public void checkExpiredSessions() {
    if(sessions.size()>maxSessions){
      List<Session> expiredSessions = sessions.values().stream()
          .sorted(Comparator.comparingLong(Session::getUid).reversed())
          .skip(maxSessions)
          .collect(Collectors.toList());

      expiredSessions.forEach(s -> {
        closeSession(s.getId());
      });
    }
  }


  @Override
  public Optional<Session> getSession(String sessionId) {
    Optional<Session> session = Optional.ofNullable(sessionId == null ? null : sessions.get(sessionId));
    return session;
  }

  @Override
  public List<Session> getAllSessions() {
    return new ArrayList<>(sessions.values());
  }

  @Override
  public Session removeSession(String sessionId) {
    if (sessionId != null) {
      synchronized (sessions) {
        LOG.info("node {} remove session={}", context.getPeerId(), sessionId);
        return sessions.remove(sessionId);
      }
    }
    return null;
  }

  @Override
  public void closeSession(String sessionId) {
    Session session = removeSession(sessionId);
    if(session!=null){
      try {
        session.close();
      } catch (Exception e) {
        LOG.warn("close session error", e);
      }
    }
  }

  @Override
  public void clearSessions() {
    for (Session session : new ArrayList<>(sessions.values())) {
      try {
        session.close();
      } catch (Exception e) {
        LOG.warn("close session error", e);
      }
    }
    sessions.clear();
  }


}
