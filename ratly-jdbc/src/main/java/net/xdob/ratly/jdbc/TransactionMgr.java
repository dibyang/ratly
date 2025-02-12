package net.xdob.ratly.jdbc;

import java.sql.SQLException;
import java.sql.Savepoint;

public interface TransactionMgr {
  Session getSession(String tx) throws SQLException;
  void initializeTx(String tx, Session session) throws SQLException;
  void addIndex(String tx, long logIndex) throws SQLException;
  void commit(String tx) throws SQLException;
  void rollback(String tx) throws SQLException;
  Savepoint setSavepoint(String tx, String name) throws SQLException;
  void releaseSavepoint(String tx, Savepoint savepoint) throws SQLException;
  void rollback(String tx, Savepoint savepoint) throws SQLException;
  void checkTimeoutTx();

  /**
   * 是否正处于事务中
   */
  boolean isTransaction();
}
