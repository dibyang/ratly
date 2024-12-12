package net.xdob.ratly.jdbc;

import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.server.raftlog.RaftLog;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.List;
import java.util.Optional;

public interface JdbcTransactionMgr {
  void initialize(String path, RaftLog raftLog);
  Connection getConnection(String tx) throws SQLException;
  void addIndex(String tx, long logIndex) throws SQLException;
  void commit(String tx) throws SQLException;
  void rollback(String tx) throws SQLException;
  Savepoint setSavepoint(String tx, String name) throws SQLException;
  void releaseSavepoint(String tx, Savepoint savepoint) throws SQLException;
  void rollback(String tx, Savepoint savepoint) throws SQLException;
  boolean needReloadTx(String tx) throws SQLException;
  Optional<TxInfo> getTxInfo(String tx);
  List<LogEntryProto> getLogs(String tx);
  void checkTimeoutTx();
  boolean hasTransaction();
}
