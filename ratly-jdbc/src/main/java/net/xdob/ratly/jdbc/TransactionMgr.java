package net.xdob.ratly.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;

public interface TransactionMgr {
  Connection getConnection(String tx) throws SQLException;
  void initializeTx(String tx, ConnSupplier connSupplier) throws SQLException;
  void addIndex(String tx, long logIndex) throws SQLException;
  void commit(String tx) throws SQLException;
  void rollback(String tx) throws SQLException;
  Savepoint setSavepoint(String tx, String name) throws SQLException;
  void releaseSavepoint(String tx, Savepoint savepoint) throws SQLException;
  void rollback(String tx, Savepoint savepoint) throws SQLException;
  void checkTimeoutTx();
}
