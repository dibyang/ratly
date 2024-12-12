package net.xdob.ratly.jdbc;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.raftlog.RaftLogIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.*;
import java.util.stream.Collectors;

public class DefaultJdbcTransactionMgr implements JdbcTransactionMgr{
  public static final String TMP = ".tmp";
  public static final int TIME_OUT = 30;
  static Logger LOG = LoggerFactory.getLogger(DefaultJdbcTransactionMgr.class);

  private final Map<String, TxInfo> txInfoMap = Maps.newConcurrentMap();

  private final ConnSupplier connSupplier;
  private RaftLog raftLog;
  private Path path;

  public DefaultJdbcTransactionMgr(ConnSupplier connSupplier) {
    this.connSupplier = connSupplier;
  }

  @Override
  public void initialize(String path, RaftLog raftLog) {
    this.path = Paths.get(path);
    this.raftLog = raftLog;
    if(!this.path.toFile().exists()){
      if(!this.path.toFile().mkdirs()){
        LOG.warn("mkdirs is fail. path={}", path);
      }
    }

  }

  @Override
  public Connection getConnection(String tx) throws SQLException {
    if(!Strings.isNullOrEmpty(tx)) {
      return getTxInfo(tx).map(TxInfo::getConnection).orElse(null);
    }
    return null;
  }

  public synchronized Optional<TxInfo> getTxInfo(String tx) {
    return Optional.ofNullable(txInfoMap.get(tx));
  }

  @Override
  public List<LogEntryProto> getLogs(String tx) {
    List<LogEntryProto> logs = new ArrayList<>();
    if(raftLog!=null) {
      getTxInfo(tx).map(TxInfo::getIndexes).ifPresent(indexes -> {
        for (Long index : indexes) {
          try {
            LogEntryProto logEntryProto = raftLog.get(index);
            logs.add(logEntryProto);
          } catch (RaftLogIOException ex) {
            LOG.warn("load log fail.", ex);
          }
        }
      });
    }
    return logs;
  }

  @Override
  public synchronized void checkTimeoutTx() {
    List<TxInfo> timeoutTxInfos = txInfoMap.values().stream().filter(e -> e.getAccessTimeOffset() > TIME_OUT)
        .collect(Collectors.toList());
    for (TxInfo txInfo : timeoutTxInfos) {
      try {
        LOG.info("tx={} release because timeout.", txInfo.getTx());
        releaseTx(txInfo.getTx());
      }catch (SQLException e){
        LOG.warn("", e);
      }
    }
  }

  TxInfo reloadTx(String tx) throws SQLException {
    try {
      File txFile = path.resolve(tx).toFile();
      if (txFile.exists()) {
        TxInfo txInfo = new TxInfo(tx);
        byte[] bytes = Files.readAllBytes(txFile.toPath());
        txInfo.setIndexes(bytes);
      }

    }catch (IOException e){
      throw new SQLException("io error", e);
    }
    return null;
  }


  synchronized void releaseTx(String tx) throws SQLException {
    TxInfo txInfo = txInfoMap.remove(tx);
    if(txInfo!=null){
      txInfo.getConnection().close();
      try {
        File[] files = path.toFile().listFiles(new FilenameFilter() {
          @Override
          public boolean accept(File dir, String name) {
            return name.startsWith(tx);
          }
        });
        if(files != null) {
          for (File file : files) {
            Files.delete(file.toPath());
          }
        }
      } catch (IOException e) {
        throw new SQLException("io error", e);
      }
    }
  }

  @Override
  public synchronized void addIndex(String tx, long logIndex) throws SQLException {
    TxInfo txInfo = getTxInfoOrThrow(tx);
    txInfo.getIndexes().add(logIndex);
    saveTxInfo(txInfo);
  }

  void saveTxInfo(TxInfo txInfo) throws SQLException {
    try {
      txInfo.updateAccessTime();
      File txFile = path.resolve(txInfo.getTx()).toFile();
      if (!txInfo.getIndexes().isEmpty()) {
        byte[] indexesBytes = txInfo.getIndexesBytes();
        if (!txFile.exists()) {
          write(txFile.toPath(), indexesBytes);
        } else if (indexesBytes.length > txFile.length()) {
          write(txFile.toPath(), indexesBytes);
        }
      }

    } catch (IOException e) {
      throw new SQLException("io error", e);
    }

  }

  private void write(Path path, byte[] bytes) throws IOException {
    Path tmp = path.resolveSibling(TMP);
    Files.write(tmp, bytes, StandardOpenOption.CREATE,
        StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING,StandardOpenOption.SYNC);
    Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
  }

  @Override
  public synchronized void commit(String tx) throws SQLException {
    TxInfo txInfo = getTxInfoOrThrow(tx);
    txInfo.getConnection().commit();
    releaseTx(tx);
  }

  @Override
  public synchronized void rollback(String tx) throws SQLException {
    TxInfo txInfo = getTxInfoOrThrow(tx);
    txInfo.getConnection().rollback();
    releaseTx(tx);
  }

  @Override
  public synchronized Savepoint setSavepoint(String tx, String name) throws SQLException {
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
  public synchronized void releaseSavepoint(String tx, Savepoint savepoint) throws SQLException {
    TxInfo txInfo = getTxInfoOrThrow(tx);
    txInfo.getConnection().releaseSavepoint(savepoint);

  }

  @Override
  public synchronized void rollback(String tx, Savepoint savepoint) throws SQLException {
    TxInfo txInfo = getTxInfoOrThrow(tx);
    txInfo.getConnection().rollback(savepoint);
  }

  @Override
  public synchronized boolean needReloadTx(String tx) throws SQLException {
    boolean reloadTx = false;
    TxInfo txInfo = getTxInfo(tx).orElse(null);
    if(txInfo==null){
      txInfo = reloadTx(tx);
      if(txInfo==null){
        txInfo = new TxInfo(tx);
      }else{
        reloadTx = true;
      }
      Connection connection = connSupplier.getConnection();
      txInfo.setConnection(connection);
      connection.setAutoCommit(false);
      txInfoMap.put(tx, txInfo);
    }
    return reloadTx;
  }


  @Override
  public boolean hasTransaction() {
    return false;
  }
}
