package net.xdob.ratly.jdbc;

import com.google.common.collect.Maps;
import net.xdob.ratly.util.GCLastTime;
import net.xdob.ratly.util.MemoizedCheckedSupplier;
import net.xdob.ratly.util.PauseLastTime;
import net.xdob.ratly.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class DefaultSessionMgr implements SessionInnerMgr{
	static final Logger LOG = LoggerFactory.getLogger(DefaultSessionMgr.class);
  public static final int SESSION_TIMEOUT = 8_000;

  private final ConcurrentMap<String, Session> sessions = Maps.newConcurrentMap();
  private final DbsContext context;
  private final AtomicBoolean expiredChecking = new AtomicBoolean();
  private final AtomicBoolean leader = new AtomicBoolean();
	private final AtomicBoolean disabled = new AtomicBoolean();
	private volatile Timestamp lastCheckTime = Timestamp.currentTime();


	public DefaultSessionMgr(DbsContext context) {
		this.context = context;
		context.getScheduler()
				.scheduleWithFixedDelay(this::checkExpiredSessions,
						0, 1000, TimeUnit.MILLISECONDS);
	}


	@Override
	public int getAvailableSessionCount(String db) {
		int count = getActiveCount(db);
		return context.getMaxConnSize(db) - count;
	}


	public int getActiveCount(String db) {
		return (int)sessions.values().stream()
				.filter(e->e.getDb().equals(db)&&e.isActive())
				.count();
	}

	@Override
  public Session newSession(String db, String user, String sessionId, MemoizedCheckedSupplier<Connection, SQLException> connSupplier) throws SQLException {
    synchronized (sessions) {
      Session session = getSession(sessionId).orElse(null);
      if (session == null) {
				session = new Session(db, user, sessionId, connSupplier, this);
				sessions.put(session.getSessionId(), session);
				LOG.info("node {} new session={}", context.getPeerId(), sessionId);
			}
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
				session.heartBeat();
			}
			return;
		}
		Timestamp lastPauseTime = context.getPauseLastTime().getLastPauseTime();
		if(lastPauseTime.getNanos()>lastCheckTime.getNanos()){
			for (Session session : sessions.values()) {
				session.heartBeat();
			}
		}
		if(expiredChecking.compareAndSet(false, true)) {
			lastCheckTime = Timestamp.currentTime();
      try {
				List<Session> expiredSessions = sessions.values().stream()
						.filter(s -> s.elapsedHeartTimeMs() > SESSION_TIMEOUT)
						.collect(Collectors.toList());
				expiredSessions.forEach(s -> {
					if(s.elapsedHeartTimeMs() > SESSION_TIMEOUT) {
						LOG.info("session close id={}, elapsedHeartTimeMs={}", s.getSessionId(), s.elapsedHeartTimeMs());
						context.closeSession(s.getDb(), s.getSessionId());
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
				session.heartBeat();
			}
		}
	}

	@Override
	public List<Long> getLastEndedTxIndexList() {
		return sessions.values().stream().map(Session::getEndTx)
				.filter(e->e>0)
				.collect(Collectors.toList());
	}

	@Override
	public long getFirstTx() {
		return sessions.values().stream().map(Session::getTx)
				.filter(e->e>0)
				.min(Long::compareTo)
				.orElse(0L);
	}


	@Override
  public Optional<Session> getSession(String sessionId) {
		return Optional.ofNullable(sessionId == null ? null : sessions.get(sessionId));
  }

  @Override
  public List<Session> getAllSessions() {
    return new ArrayList<>(sessions.values())
				.stream()
				.sorted(Comparator.comparing(Session::getDb)
						.thenComparing(Comparator.comparing(Session::getSessionId)))
				.collect(Collectors.toList());
  }

	@Override
	public List<Session> getAllSessions(String db) {
		return getAllSessions().stream()
				.filter(e->e.getDb().equals(db))
				.collect(Collectors.toList());
	}


	public Session removeSession(String sessionId) {
    if (sessionId != null) {
      synchronized (sessions) {
        return sessions.remove(sessionId);
      }
    }
    return null;
  }

  @Override
  public boolean closeSession(String sessionId, long index) {
    Session session = removeSession(sessionId);
    if(session!=null){
      try {
				if(session.isTransaction()){
					try {
						session.rollback(index);
					} catch (SQLException ignore) {
					}
				}
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


	@Override
	public void close() {
	}
}
