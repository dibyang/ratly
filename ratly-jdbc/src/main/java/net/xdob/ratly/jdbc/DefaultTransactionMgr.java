package net.xdob.ratly.jdbc;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.*;
import java.util.stream.Collectors;

public class DefaultTransactionMgr implements TransactionMgr {
  public static final int TIME_OUT = 30;
  static Logger LOG = LoggerFactory.getLogger(DefaultTransactionMgr.class);

  private final Map<String, TxInfo> txInfoMap = Maps.newConcurrentMap();



  public DefaultTransactionMgr() {
  }


  @Override
  public Connection getConnection(String tx) throws SQLException {
    if(!Strings.isNullOrEmpty(tx)) {
      return getTxInfo(tx).map(TxInfo::getConnection).orElse(null);
    }
    return null;
  }

  @Override
  public void initializeTx(String tx, ConnSupplier connSupplier) throws SQLException {
    synchronized (txInfoMap) {
      TxInfo txInfo = getTxInfo(tx).orElse(null);
      if (txInfo == null) {
        txInfo = new TxInfo(tx);
        Connection connection = connSupplier.getConnection();
        txInfo.setConnection(connection);
        connection.setAutoCommit(false);
        txInfoMap.put(tx, txInfo);
      }
    }
  }

  public Optional<TxInfo> getTxInfo(String tx) {
    return Optional.ofNullable(txInfoMap.get(tx));
  }

  @Override
  public void checkTimeoutTx() {
    synchronized (txInfoMap) {
      List<TxInfo> timeoutTxInfos = txInfoMap.values().stream().filter(e -> e.getAccessTimeOffset() > TIME_OUT)
          .collect(Collectors.toList());
      for (TxInfo txInfo : timeoutTxInfos) {
        try {
          LOG.info("tx={} release because timeout.", txInfo.getTx());
          releaseTx(txInfo.getTx());
        } catch (SQLException e) {
          LOG.warn("", e);
        }
      }
    }
  }

  @Override
  public boolean isTransaction() {
    return !txInfoMap.isEmpty();
  }

  void releaseTx(String tx) throws SQLException {
    synchronized (txInfoMap) {
      TxInfo txInfo = txInfoMap.remove(tx);
      if (txInfo != null) {
        txInfo.getConnection().close();
      }
    }
  }

  @Override
  public void addIndex(String tx, long logIndex) throws SQLException {
    TxInfo txInfo = getTxInfoOrThrow(tx);
    txInfo.getIndexes().add(logIndex);
    txInfo.updateAccessTime();
  }



  @Override
  public void commit(String tx) throws SQLException {
    TxInfo txInfo = getTxInfoOrThrow(tx);
    txInfo.getConnection().commit();
    releaseTx(tx);
  }

  @Override
  public void rollback(String tx) throws SQLException {
    TxInfo txInfo = getTxInfoOrThrow(tx);
    txInfo.getConnection().rollback();
    releaseTx(tx);
  }

  @Override
  public Savepoint setSavepoint(String tx, String name) throws SQLException {
    TxInfo txInfo = getTxInfoOrThrow(tx);
    Savepoint savepoint = null;
    if(name.isEmpty()) {
      savepoint = txInfo.getConnection().setSavepoint();
    }else {
      savepoint = txInfo.getConnection().setSavepoint(name);
    }
    return savepoint;
  }

  private TxInfo getTxInfoOrThrow(String tx) throws SQLException {
    TxInfo txInfo = getTxInfo(tx).orElse(null);
    if(txInfo==null){
      throw new SQLException("Jdbc Transaction not find. tx="+ tx);
    }
    return txInfo;
  }

  @Override
  public void releaseSavepoint(String tx, Savepoint savepoint) throws SQLException {
    TxInfo txInfo = getTxInfoOrThrow(tx);
    txInfo.getConnection().releaseSavepoint(savepoint);
  }

  @Override
  public void rollback(String tx, Savepoint savepoint) throws SQLException {
    TxInfo txInfo = getTxInfoOrThrow(tx);
    txInfo.getConnection().rollback(savepoint);
  }

}
