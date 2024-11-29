package net.xdob.ratly.jdbc;

import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.statemachine.TransactionContext;
import net.xdob.ratly.statemachine.impl.BaseStateMachine;

import java.io.IOException;
import java.sql.*;
import java.util.concurrent.CompletableFuture;

public class JdbcStateMachine extends BaseStateMachine {

  public static final String DEFAULT_USER = "sa";
  public static final String DEFAULT_PASSWORD = "";
  private final String path;
  private final String db;
  private final String username;
  private final String password;

  private Connection connection;

  public JdbcStateMachine(String path, String db, String username, String password) {
    this.path = path;
    this.db = db;
    this.username = username;
    this.password = password;
  }

  public JdbcStateMachine(String path, String db){
    this(path,db, DEFAULT_USER, DEFAULT_PASSWORD);
  }

  /**
   * 初始化数据库连接
   */
  public void initialize() {
    try {
      if (path != null) {
        // 基于存储目录初始化
        connection = DriverManager.getConnection(
            "jdbc:h2:" + path + "/"+db, username, password);
        restoreFromSnapshot();
      } else {
        throw new IllegalStateException("Invalid state: No initialization parameters provided.");
      }
    } catch (SQLException | IOException e) {
      throw new RuntimeException("Failed to initialize H2 database", e);
    }
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    LogEntryProto entry = trx.getLogEntry();
    String sql = new String(entry.getStateMachineLogEntry().getLogData().toByteArray());

    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);

      // 如果日志数量达到一定阈值，生成快照
      if (trx.getLogEntry().getIndex() % 100 == 0) {
        createSnapshot();
      }

      return CompletableFuture.completedFuture(Message.valueOf("Executed: " + sql));
    } catch (SQLException e) {
      e.printStackTrace();
      return CompletableFuture.completedFuture(Message.valueOf("Error executing SQL: " + e.getMessage()));
    }
  }

  // 新增方法：处理查询型 SQL
  public String executeQuery(String sql) {
    StringBuilder result = new StringBuilder();
    try (Statement statement = connection.createStatement();
         ResultSet rs = statement.executeQuery(sql)) {

      // 获取结果集的元数据
      ResultSetMetaData metaData = rs.getMetaData();
      int columnCount = metaData.getColumnCount();

      // 添加列名到结果
      for (int i = 1; i <= columnCount; i++) {
        result.append(metaData.getColumnName(i)).append("\t");
      }
      result.append("\n");

      // 遍历结果集
      while (rs.next()) {
        for (int i = 1; i <= columnCount; i++) {
          result.append(rs.getString(i)).append("\t");
        }
        result.append("\n");
      }
    } catch (SQLException e) {
      result.append("Error executing query: ").append(e.getMessage());
    }
    return result.toString();
  }

  private void createSnapshot() throws SQLException {
    if (path == null) {
      return; // 基于 JDBC 模式不支持快照
    }
//    try (Statement statement = connection.createStatement()) {
//      statement.execute("SCRIPT TO '" + snapshotFile + "'");
//    }
  }

  private void restoreFromSnapshot() throws IOException, SQLException {

//    File file = new File(snapshotFile);
//    if (file.exists()) {
//      try (Statement statement = connection.createStatement()) {
//        statement.execute("RUNSCRIPT FROM '" + snapshotFile + "'");
//      }
//    }
  }

  @Override
  public long takeSnapshot() throws IOException {
    try {
      createSnapshot();
    } catch (SQLException e) {
      throw new IOException("Failed to create snapshot", e);
    }
    return super.takeSnapshot();
  }

  @Override
  public void close() throws IOException {
    super.close();
    try {
      if (connection != null && !connection.isClosed()) {
        connection.close();
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

}
