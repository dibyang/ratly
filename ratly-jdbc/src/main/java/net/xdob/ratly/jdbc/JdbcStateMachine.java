package net.xdob.ratly.jdbc;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import net.xdob.ratly.jdbc.sql.*;
import net.xdob.ratly.proto.jdbc.*;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.storage.RaftStorage;
import net.xdob.ratly.statemachine.TransactionContext;
import net.xdob.ratly.statemachine.impl.BaseStateMachine;
import org.apache.commons.dbcp2.BasicDataSource;

import org.h2.Driver;
import net.xdob.ratly.fasts.serialization.FSTConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class JdbcStateMachine extends BaseStateMachine {
  static final Logger LOG = LoggerFactory.getLogger(JdbcStateMachine.class);

  public static final String DEFAULT_USER = "remote";
  public static final String DEFAULT_PASSWORD = "hhrhl2016";


  private final BasicDataSource dataSource = new BasicDataSource();

  private final FSTConfiguration fasts = FSTConfiguration.createDefaultConfiguration();

  private final String db;
  private final Map<String,Method> methodCache = Maps.newConcurrentMap();
  private JdbcTransactionMgr jdbcTransactionMgr;
  private String path;

  private String username = DEFAULT_USER;
  private String password = DEFAULT_PASSWORD;
  private ScheduledExecutorService scheduler;

  public JdbcStateMachine(String db) {
    this.db = db;
  }


  @Override
  public void initialize(RaftServer server, RaftGroupId groupId,
                         RaftStorage raftStorage) throws IOException {
    super.initialize(server, groupId, raftStorage);
    initialize(raftStorage, null);

  }

  /**
   * 初始化数据库连接
   */
  public void initialize(RaftStorage raftStorage, RaftLog raftLog) {
    try {
      this.path = Paths.get(raftStorage.getStorageDir().getRoot().getPath(), "db").toString();
      DriverManager.registerDriver(new Driver());
      // 基于存储目录初始化
      dataSource.setUrl("jdbc:h2:file:" + path + "/"+db+";AUTO_SERVER=TRUE");
      dataSource.setDriverClassName("org.h2.Driver");
      dataSource.setUsername(username);
      dataSource.setPassword(password);
      //连接验证方式超时时间
      dataSource.setValidationQueryTimeout(100000);
      //连接验证方式
      dataSource.setValidationQuery("SELECT 1");
      //创建连接的时候是否进行验证
      dataSource.setTestOnCreate(false);
      //从连接池中获取的时候验证
      dataSource.setTestOnBorrow(true);
      //空闲连接进行验证
      dataSource.setTestWhileIdle(false);
      //换回连接池进行验证
      dataSource.setTestOnReturn(true);
      //设置空闲连接验证时间间隔
      dataSource.setTimeBetweenEvictionRunsMillis(-1);
      //设置验证空闲连接的数量
      dataSource.setNumTestsPerEvictionRun(3);
      //设置空闲连接验证间隔（不生效的）
      dataSource.setMinEvictableIdleTimeMillis(1800000);
      //初始化连接数量
      dataSource.setInitialSize(4);
      //最大连接数量
      dataSource.setMaxTotal(6);
      //设置默认超时时间
      dataSource.setDefaultQueryTimeout(6);

      this.jdbcTransactionMgr = new DefaultJdbcTransactionMgr(()-> dataSource.getConnection());
      this.jdbcTransactionMgr.initialize(Paths.get(path,db, "tx").toString(), raftLog);
      this.scheduler = Executors.newSingleThreadScheduledExecutor();
      scheduler.scheduleAtFixedRate(()->{
        jdbcTransactionMgr.checkTimeoutTx();
      },10, 10, TimeUnit.SECONDS);
      restoreFromSnapshot();
    } catch (SQLException | IOException e) {
      throw new RuntimeException("Failed to initialize H2 database", e);
    }
  }

  @Override
  public void notifyNotLeader(Collection<TransactionContext> pendingEntries) throws IOException {

  }

  @Override
  public CompletableFuture<Message> query(Message request) {
    QueryReplyProto.Builder builder = QueryReplyProto.newBuilder();
    try {
      QueryRequestProto queryRequest = QueryRequestProto.parseFrom(request.getContent());
      LOG.info("sql={}", queryRequest.getSql());
      String tx = queryRequest.getTx();
      Connection conn = jdbcTransactionMgr.getConnection(tx);
      if(conn==null){
        try (Connection connection = dataSource.getConnection()) {
          query(queryRequest, connection, builder);
        } catch (SQLException e) {
          builder.setEx(getSqlExceptionProto(e));
        }
      }else{
        try {
          query(queryRequest, conn, builder);
        } catch (SQLException e) {
          builder.setEx(getSqlExceptionProto(e));
        }
      }
    } catch (SQLException e) {
      builder.setEx(getSqlExceptionProto(e));
    } catch (Exception e) {
      LOG.warn("", e);
    }
    return CompletableFuture.completedFuture(Message.valueOf(builder.build()));
  }

  private void query(QueryRequestProto queryRequest, Connection connection, QueryReplyProto.Builder builder) throws SQLException {
    if(queryRequest.getType().equals(QueryType.check)){
      doCheck(connection, queryRequest.getSql());
    }else if(queryRequest.getType().equals(QueryType.meta)){
      doMeta(queryRequest, connection, queryRequest.getSql(), builder);
    }else if(queryRequest.getType().equals(QueryType.query)){
      doQuery(connection, queryRequest, builder);
    }
  }

  private void doQuery(Connection connection, QueryRequestProto queryRequest,
                       QueryReplyProto.Builder builder) throws SQLException {
    if(Sender.prepared.equals(queryRequest.getSender())
    ||Sender.callable.equals(queryRequest.getSender())){
      try(CallableStatement ps = connection.prepareCall(queryRequest.getSql())) {
        ps.setFetchDirection(queryRequest.getFetchDirection());
        ps.setFetchSize(queryRequest.getFetchSize());
        if(queryRequest.hasParam()){
          for (ParamProto paramProto : queryRequest.getParam().getParamList()) {
            Object value = fasts.asObject(paramProto.getValue().toByteArray());
            ps.setObject(paramProto.getIndex(), value);
          }
        }

        ResultSet rs = ps.executeQuery();
        builder.setRs(getByteString(new SimpleResultSet(rs)));
      }
    }else{
      try(Statement s = connection.createStatement()) {
        ResultSet rs = s.executeQuery(queryRequest.getSql());
        builder.setRs(getByteString(new SimpleResultSet(rs)));
      }
    }
  }

  private void doCheck(Connection connection, String sql) throws SQLException {
    try(PreparedStatement ps = connection.prepareStatement(sql)) {
      //check sql
    }
  }

  private void doMeta(QueryRequestProto queryRequest, Connection connection, String sql, QueryReplyProto.Builder builder) throws SQLException {
    if(Sender.connection.equals(queryRequest.getSender())){
      DatabaseMetaData metaData = connection.getMetaData();
      try {
        Method method = getMethod(metaData, sql);
        Object[] args = (Object[])fasts.asObject(queryRequest.getParam().getParam(0).getValue().toByteArray());
        Object o = method.invoke(metaData, args);
        SimpleResultSet resultSet;
        if (ResultSet.class.isAssignableFrom(method.getReturnType())) {
          resultSet = new SimpleResultSet((ResultSet) o);
        } else {
          SimpleResultSetMetaData resultSetMetaData = buildResultSetMetaData(method.getReturnType());
          resultSet = new SimpleResultSet(resultSetMetaData);
          resultSet.addRows(new SimpleRow(1).setValue(0, o));
        }
        builder.setRs(getByteString(resultSet));
      } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
        throw new SQLException(e);
      }
    }else {
      try(PreparedStatement ps = connection.prepareStatement(sql)) {
        SimpleResultSetMetaData resultSetMetaData = new SimpleResultSetMetaData(ps.getMetaData());
        builder.setRsMeta(getByteString(resultSetMetaData));
        SimpleParameterMetaData parameterMetaData = new SimpleParameterMetaData(ps.getParameterMetaData());
        builder.setParamMeta(getByteString(parameterMetaData));
      }
    }
  }

  private SimpleResultSetMetaData buildResultSetMetaData(Class<?> returnType) {
    SimpleResultSetMetaData resultSetMetaData = new SimpleResultSetMetaData();
    if (returnType.equals(Boolean.class)
        || returnType.equals(boolean.class)) {
      resultSetMetaData.addColumn("COL1", JDBCType.BOOLEAN.getVendorTypeNumber(), 0, 0);
    } else if (returnType.equals(Integer.class)
        || returnType.equals(int.class)) {
      resultSetMetaData.addColumn("COL1", JDBCType.INTEGER.getVendorTypeNumber(), 10, 0);
    } else if (returnType.equals(Long.class)
        || returnType.equals(long.class)) {
      resultSetMetaData.addColumn("COL1", JDBCType.BIGINT.getVendorTypeNumber(), 20, 0);
    } else if (returnType.equals(String.class)) {
      resultSetMetaData.addColumn("COL1", JDBCType.VARCHAR.getVendorTypeNumber(), 0, 0);
    } else {
      resultSetMetaData.addColumn("COL1", JDBCType.VARCHAR.getVendorTypeNumber(), 0, 0);
    }
    return resultSetMetaData;
  }

  private Method getMethod(DatabaseMetaData metaData, String name) throws NoSuchMethodException {
    Method method = methodCache.get(name);
    if(method==null){
      Method[] methods = metaData.getClass().getMethods();
      for (Method m : methods) {
        if(m.getName().equals(name)){
          method = m;
        }
        methodCache.put(m.getName(), m);
      }
    }
    return method;
  }

  private static String buildKey(String name, Class<?>[] parameterTypes) {
    return name + "_" + Arrays.toString(parameterTypes);
  }

  private ByteString getByteString(Object value) {
    return ByteString.copyFrom(fasts.asByteArray(value));
  }

  private SQLExceptionProto getSqlExceptionProto(SQLException e) {
    SQLExceptionProto.Builder builder = SQLExceptionProto.newBuilder();
    if(e.getMessage()!=null){
      builder.setReason(e.getMessage());
    }
    if(e.getSQLState()!=null){
      builder.setState(e.getSQLState());
    }
    builder.setErrorCode(e.getErrorCode())
        .setStacktrace(getByteString(e.getStackTrace()));
    return builder.build();
  }


  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {

    UpdateReplyProto.Builder builder = UpdateReplyProto.newBuilder();
    LogEntryProto entry = trx.getLogEntry();
    try {
      UpdateRequestProto updateRequest = UpdateRequestProto.parseFrom(entry.getStateMachineLogEntry().getLogData());
      String tx = updateRequest.getTx();
      if(tx.isEmpty()) {
        try (Connection connection = dataSource.getConnection()) {
          executeUpdate(updateRequest, connection, builder);
        } catch (SQLException e) {
          builder.setEx(getSqlExceptionProto(e));
        }
      }else {
        if (jdbcTransactionMgr.needReloadTx(tx)) {
          List<LogEntryProto> logs = jdbcTransactionMgr.getLogs(tx);
          for (LogEntryProto log : logs) {
            UpdateReplyProto.Builder builder2 = UpdateReplyProto.newBuilder();
            UpdateRequestProto request = UpdateRequestProto.parseFrom(log.getStateMachineLogEntry().getLogData());
            applyLog(request, builder2);
          }
        }
        applyLog(updateRequest, builder);
        if (!UpdateType.commit.equals(updateRequest.getType())
          &&!UpdateType.rollback.equals(updateRequest.getType()))
        {
          jdbcTransactionMgr.addIndex(tx, entry.getIndex());
        }
      }
    } catch (InvalidProtocolBufferException e) {
      LOG.warn("", e);
    } catch (SQLException e) {
      builder.setEx(getSqlExceptionProto(e));
    }
    updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
    return CompletableFuture.completedFuture(Message.valueOf(builder.build()));
  }

  private void applyLog(UpdateRequestProto updateRequest, UpdateReplyProto.Builder builder) throws SQLException {
    String tx = updateRequest.getTx();
    if(UpdateType.execute.equals(updateRequest.getType())){
      Connection connection = jdbcTransactionMgr.getConnection(tx);
      try {
        executeUpdate(updateRequest, connection, builder);
      } catch (SQLException e) {
        builder.setEx(getSqlExceptionProto(e));
      }
    }else if(UpdateType.commit.equals(updateRequest.getType())){
      jdbcTransactionMgr.commit(tx);
    }else if(UpdateType.rollback.equals(updateRequest.getType())){
      jdbcTransactionMgr.rollback(tx);
    }else if(UpdateType.savepoint.equals(updateRequest.getType())){
      String name = updateRequest.getSql();
      Savepoint savepoint = jdbcTransactionMgr.setSavepoint(tx, name);
      builder.setSavepoint(SavepointProto.newBuilder()
          .setId(savepoint.getSavepointId())
          .setName(savepoint.getSavepointName()).build());
    }else if(UpdateType.releaseSavepoint.equals(updateRequest.getType())){
      SavepointProto savepoint = updateRequest.getSavepoint();
      jdbcTransactionMgr.releaseSavepoint(tx, new JdbcSavepoint(savepoint.getId(), savepoint.getName()));
    }else if(UpdateType.rollbackSavepoint.equals(updateRequest.getType())){
      SavepointProto savepoint = updateRequest.getSavepoint();
      jdbcTransactionMgr.rollback(tx, new JdbcSavepoint(savepoint.getId(), savepoint.getName()));
    }
  }

  private void executeUpdate(UpdateRequestProto updateRequest, Connection connection,
                         UpdateReplyProto.Builder builder) throws SQLException {
    if (Sender.prepared.equals(updateRequest.getSender())
        || Sender.callable.equals(updateRequest.getSender())) {
      update4Prepared(connection, updateRequest, builder);
    } else {
      update4Statement(connection, updateRequest, builder);
    }
  }

  void update4Prepared(Connection connection,
                       UpdateRequestProto updateRequest,
                       UpdateReplyProto.Builder builder) throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement(updateRequest.getSql())) {
      if (updateRequest.getBatchParamCount() > 0) {
        for (int i = 0; i < updateRequest.getBatchParamCount(); i++) {
          ParamListProto paramListProto = updateRequest.getBatchParam(i);
          setParams(ps, paramListProto);
          ps.addBatch();
        }
        long[] updateCounts = ps.executeLargeBatch();
        for (int i = 0; i < updateCounts.length; i++) {
          builder.setCounts(i, updateCounts[i]);
        }
      } else {
        setParams(ps, updateRequest.getParam());
        long updateCount = ps.executeLargeUpdate();
        builder.setCount(updateCount);
      }
    }
  }

  private void setParams(PreparedStatement ps, ParamListProto paramList) throws SQLException {
    if(paramList.getParamCount()>0) {
      for (int i = 0; i < paramList.getParamCount(); i++) {
        ParamProto param = paramList.getParam(i);
        ps.setObject(param.getIndex(), fasts.asObject(param.getValue().toByteArray()));
      }
    }
  }

  void update4Statement(Connection connection,
                        UpdateRequestProto updateRequest,
                        UpdateReplyProto.Builder builder) throws SQLException {
    try (Statement s = connection.createStatement()) {
      if (updateRequest.getBatchSqlCount() > 0) {
        for (int i = 0; i < updateRequest.getBatchSqlCount(); i++) {
          s.addBatch(updateRequest.getBatchSql(i));
        }
        long[] updateCounts = s.executeLargeBatch();
        for (int i = 0; i < updateCounts.length; i++) {
          builder.setCounts(i, updateCounts[i]);
        }
      } else {
        LOG.info("s type={}", s.getClass());
        long updateCount = s.executeLargeUpdate(updateRequest.getSql());
        builder.setCount(updateCount);
      }
    }
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
      if (dataSource.isClosed()) {
        dataSource.close();
      }
    } catch (SQLException e) {
      LOG.warn("", e);
    }
  }



}
