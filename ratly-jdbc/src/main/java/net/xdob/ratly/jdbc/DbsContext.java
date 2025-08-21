package net.xdob.ratly.jdbc;

import net.xdob.ratly.protocol.SerialSupport;
import net.xdob.ratly.security.crypto.password.PasswordEncoder;
import net.xdob.ratly.security.RsaHelper;
import net.xdob.ratly.statemachine.SnapshotInfo;

import java.util.concurrent.ScheduledExecutorService;

public interface DbsContext {
  String getPeerId();
  ScheduledExecutorService getScheduler();
  SnapshotInfo getLatestSnapshot();
  boolean isLeader();
  PasswordEncoder getPasswordEncoder();
  RsaHelper getRsaHelper();
  void updateDbs();
  void closeSession(String db, String sessionId);
  void stopServerState();
}
