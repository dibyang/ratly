package net.xdob.jdbc;

import com.google.common.collect.Maps;
import net.xdob.jdbc.exception.SessionIdAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class DefaultSessionMgr implements SessionMgr{
	static final Logger LOG = LoggerFactory.getLogger(DefaultSessionMgr.class);
  public static final int SESSION_TIMEOUT = 10_000;

  private final ConcurrentMap<String, Session> sessions = Maps.newConcurrentMap();
  private final DbContext context;
  private final AtomicBoolean expiredChecking = new AtomicBoolean();
  private final AtomicBoolean leader = new AtomicBoolean();
	private final AtomicBoolean disabled = new AtomicBoolean();

	public DefaultSessionMgr(DbContext context) {
		this.context = context;
		context.getScheduler()
				.scheduleWithFixedDelay(this::checkExpiredSessions,
						1, 1, TimeUnit.SECONDS);
	}



  @Override
  public Session newSession(String db, String user, String sessionId, ConnSupplier connSupplier) throws SQLException {
    synchronized (sessions) {
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
		if(disabled.get()){
			return;
		}
		if(!context.getPeerId().equals(context.getLeaderId())){
			leader.set(false);
			return;
		}
		if(leader.compareAndSet(false, true)){
			for (Session session : sessions.values()) {
				session.updateLastHeartTime();
			}
		}
    if(expiredChecking.compareAndSet(false, true)) {
      try {
				List<Session> expiredSessions = sessions.values().stream()
						.filter(s -> s.elapsedHeartTimeMs()> SESSION_TIMEOUT)
						.collect(Collectors.toList());
				expiredSessions.forEach(s -> {
					if(s.elapsedHeartTimeMs()> SESSION_TIMEOUT) {
						LOG.info("session close id={}, elapsedHeartTimeMs={}", s.getSessionId(), s.elapsedHeartTimeMs());
						context.closeSession(s.getSessionId());
					}
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
	public void setDisabled(boolean disabled) {
		this.disabled.set(disabled);
		if(!this.disabled.get()){
			for (Session session : sessions.values()) {
				session.updateLastHeartTime();
			}
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


  private Session removeSession(String sessionId) {
    if (sessionId != null) {
      synchronized (sessions) {
        return sessions.remove(sessionId);
      }
    }
    return null;
  }

  @Override
  public boolean closeSession(String sessionId) {
    Session session = removeSession(sessionId);
    if(session!=null){
      try {
        session.close();
      } catch (Exception e) {
        LOG.warn("close session error", e);
      }
			return true;
    }
		return false;
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
  }


}
