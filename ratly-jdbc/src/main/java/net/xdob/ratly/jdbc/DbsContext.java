package net.xdob.ratly.jdbc;

import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.security.crypto.password.PasswordEncoder;
import net.xdob.ratly.security.RsaHelper;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.util.PauseLastTime;
import net.xdob.ratly.util.Timestamp;

import java.util.concurrent.ScheduledExecutorService;

public interface DbsContext {
	String getLeaderId();
  String getPeerId();
  ScheduledExecutorService getScheduler();
  PasswordEncoder getPasswordEncoder();
  RsaHelper getRsaHelper();
  void updateDbs();
  //void closeSession(String db, String sessionId);
  void stopServerState();
  /**
   * 获取状态机的最新应用索引
   */
  TermIndex getLastAppliedTermIndex();
	RaftClient getRaftClient();
	void closeSession(String db, String sessionId);
	SessionMgr getSessionMgr();
	int getMaxConnSize(String db);
	PauseLastTime getPauseLastTime();
}
