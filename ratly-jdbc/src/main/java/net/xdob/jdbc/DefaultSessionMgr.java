package net.xdob.jdbc;

import com.google.common.collect.Maps;
import net.xdob.jdbc.exception.SessionIdAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class DefaultSessionMgr implements SessionMgr{
	static final Logger LOG = LoggerFactory.getLogger(DefaultSessionMgr.class);
  public static final int DEFAULT_MAX_SESSIONS = 16;
	public static final int SESSION_TIMEOUT = 30_000;

	private int maxSessions = DEFAULT_MAX_SESSIONS;
  private final ConcurrentMap<String, Session> sessions = Maps.newConcurrentMap();
  private final DbContext context;
  private final AtomicBoolean expiredChecking = new AtomicBoolean();

	public DefaultSessionMgr(DbContext context) {
		this.context = context;
	}

  public int getMaxSessions() {
    return maxSessions;
  }

  public DefaultSessionMgr setMaxSessions(int maxSessions) {
    this.maxSessions = Math.max(maxSessions, DEFAULT_MAX_SESSIONS);
    return this;
  }

  @Override
  public Session newSession(String db, String user, String sessionId, ConnSupplier connSupplier) throws SQLException {
    synchronized (sessions) {
			if(sessions.size()>=maxSessions){
				LOG.warn("node {} max sessions reached, max={}", context.getPeerId(), maxSessions);
				throw new SQLException("can not open session, max sessions reached");
			}
      Session session = getSession(sessionId).orElse(null);
      if (session != null) {
        throw new SessionIdAlreadyExistsException(sessionId);
      }
      session = new Session(db, user, sessionId, connSupplier.getConnection(), context);
      sessions.put(session.getSessionId(), session);
      LOG.info("node {} new session={}", context.getPeerId(), sessionId);

      return session;
    }
  }

  public void checkExpiredSessions() {
		if(!context.getPeerId().equals(context.getLeaderId())){
			return;
		}
    if(expiredChecking.compareAndSet(false, true)) {
      try {
				List<Session> expiredSessions = sessions.values().stream()
						.filter(s -> s.elapsedHeartTimeMs()> SESSION_TIMEOUT)
						.collect(Collectors.toList());
				expiredSessions.forEach(s -> {
					context.closeSession(s.getSessionId());
				});
      } finally {
        expiredChecking.set(false);
      }
    }
  }

	@Override
	public boolean isTransaction() {
		return sessions.values().stream().anyMatch(Session::isTransaction);
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
