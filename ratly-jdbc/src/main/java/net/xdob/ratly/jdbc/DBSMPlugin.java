package net.xdob.ratly.jdbc;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import net.xdob.ratly.io.MD5Hash;
import net.xdob.ratly.jdbc.exception.NoSessionException;
import net.xdob.ratly.jdbc.sql.*;
import net.xdob.ratly.proto.jdbc.*;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.storage.FileInfo;
import net.xdob.ratly.server.storage.RaftStorage;
import net.xdob.ratly.server.util.RsaHelper;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.statemachine.impl.BaseSMPlugin;
import net.xdob.ratly.statemachine.impl.FileListStateMachineStorage;
import net.xdob.ratly.statemachine.impl.SMPluginContext;
import net.xdob.ratly.util.MD5FileUtil;
import net.xdob.ratly.util.ZipUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.h2.Driver;
import org.h2.util.JdbcUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class DBSMPlugin extends BaseSMPlugin {

  public static final String DEFAULT_USER = "remote";
  public static final String DEFAULT_PASSWORD = "hhrhl2016";
  public static final String DB = "db";
  public static final String DB2ZIP = "db.zip";


  private final BasicDataSource dataSource = new BasicDataSource();
  private final String db;
  private final Map<String, Method> methodCache = Maps.newConcurrentMap();
  private TransactionMgr transactionMgr;

  private String username = DEFAULT_USER;
  private String password = DEFAULT_PASSWORD;

  private Path dbStore;
  private SMPluginContext context;
  private DefaultSessionMgr sessionMgr = new DefaultSessionMgr();

  private final RsaHelper rsaHelper = new RsaHelper();

  public DBSMPlugin(String db) {
    super(DB);
    this.db = db;
  }

  @Override
  public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage,
                         SMPluginContext context) throws IOException {
    /**
     * 初始化数据库连接
     */
    try {
      this.context = context;
      this.dbStore = Paths.get(raftStorage.getStorageDir().getRoot().getPath(), "db");
      DriverManager.registerDriver(new Driver());
      String dbPath = dbStore.resolve(db).toString();
      // 基于存储目录初始化
      String url = "jdbc:h2:file:" + dbPath;
      dataSource.setUrl(url + ";AUTO_SERVER=TRUE");
      dataSource.setDriverClassName("org.h2.Driver");
      dataSource.setUsername(username);
      dataSource.setPassword(password);
      //连接验证方式超时时间
      dataSource.setValidationQueryTimeout(Duration.ofSeconds(10));
      //连接验证方式
      //dataSource.setValidationQuery("SELECT 1");
      //创建连接的时候是否进行验证
      dataSource.setTestOnCreate(false);
      //从连接池中获取的时候验证
      dataSource.setTestOnBorrow(true);
      //空闲连接进行验证
      dataSource.setTestWhileIdle(false);
      //换回连接池进行验证
      dataSource.setTestOnReturn(true);
      //设置空闲连接验证时间间隔
      dataSource.setDurationBetweenEvictionRuns(Duration.ofSeconds(60));
      //设置验证空闲连接的数量
      dataSource.setNumTestsPerEvictionRun(3);
      //设置空闲连接验证间隔（不生效的）
      dataSource.setMinEvictableIdle(Duration.ofSeconds(600));
      //初始化连接数量
      dataSource.setInitialSize(4);
      //最大连接数量
      dataSource.setMaxTotal(16);
      //设置默认超时时间
      dataSource.setDefaultQueryTimeout(Duration.ofSeconds(10));

      this.transactionMgr = new DefaultTransactionMgr();

      context.getScheduler().scheduleAtFixedRate(()->{
        transactionMgr.checkTimeoutTx();
      },10, 10, TimeUnit.SECONDS);
      boolean exists = dbStore.resolve(db + ".mv.db").toFile().exists();
      if(!exists||hasDBError(url, username, password)){
        restoreFromSnapshot(context.getLatestSnapshot());
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to initialize H2 database", e);
    }
  }

  private boolean hasDBError(String url, String user, String password) throws SQLException {
    try (Connection conn = JdbcUtils.getConnection(null, url,user,password)){
      PreparedStatement ps = conn.prepareStatement("show tables");
      ps.execute();
      ResultSet rs = ps.executeQuery();
    } catch(SQLException e){
      return true;
    }
    return false;
  }

  @Override
  public void reinitialize() throws IOException {
  }

  @Override
  public Object query(Message request) {
    QueryReply queryReply = new QueryReply();
    try {
      QueryRequest queryRequest = context.as(request.getContent());
      Sender sender = queryRequest.getSender();
      QueryType type = queryRequest.getType();
      if(Sender.connection.equals(sender)&&QueryType.check.equals(type)){
        String user = queryRequest.getUser();
        String password = rsaHelper.decrypt(queryRequest.getPassword());
        if(!this.password.equals(password)){
          throw new SQLInvalidAuthorizationSpecException();
        }
        Session session = sessionMgr.newSession(user, password);
        LOG.info("open session user={}, id={}", user, session.getId());
        queryReply.setRs(session.getId());

      }else{
        String sessionId = queryRequest.getSession();
        Session session = sessionMgr.getSession(sessionId)
            .orElseThrow(()->new NoSessionException(sessionId));
        String tx = session.getTx();
        Connection conn = transactionMgr.getConnection(tx);
        if(conn==null){
          try (Connection connection = dataSource.getConnection()) {
            query(queryRequest, connection, queryReply);
          } catch (SQLException e) {
            queryReply.setEx(e);
          }
        }else{
          try {
            query(queryRequest, conn, queryReply);
          } catch (SQLException e) {
            queryReply.setEx(e);
          }
        }
      }

    } catch (SQLException e) {
      queryReply.setEx(e);
    } catch (Exception e) {
      LOG.warn("", e);
    }
    return queryReply;
  }


  private void query(QueryRequest queryRequest, Connection connection, QueryReply queryReply) throws SQLException {
    if(queryRequest.getType().equals(QueryType.check)){
      doSqlCheck(connection, queryRequest.getSql());
    }else if(queryRequest.getType().equals(QueryType.meta)){
      doMeta(queryRequest, connection, queryRequest.getSql(), queryReply);
    }else if(queryRequest.getType().equals(QueryType.query)){
      doQuery(connection, queryRequest, queryReply);
    }
  }

  private void doQuery(Connection connection, QueryRequest queryRequest,
                       QueryReply queryReply) throws SQLException {
    if(Sender.prepared.equals(queryRequest.getSender())
        ||Sender.callable.equals(queryRequest.getSender())){
      try(CallableStatement ps = connection.prepareCall(queryRequest.getSql())) {
        ps.setFetchDirection(queryRequest.getFetchDirection());
        ps.setFetchSize(queryRequest.getFetchSize());
        if(!queryRequest.getParams().isEmpty()){
          for (Parameter param : queryRequest.getParams().getParameters()) {
            Object value = param.getValue();
            ps.setObject(param.getIndex(), value);
          }
        }

        ResultSet rs = ps.executeQuery();
        queryReply.setRs(new SerialResultSet(rs));
      }
    }else{
      try(Statement s = connection.createStatement()) {
        ResultSet rs = s.executeQuery(queryRequest.getSql());
        queryReply.setRs(new SerialResultSet(rs));
      }
    }
  }

  private void doSqlCheck(Connection connection, String sql) throws SQLException {
    try(PreparedStatement ps = connection.prepareStatement(sql)) {
      //check sql
    }
  }

  private void doMeta(QueryRequest queryRequest, Connection connection, String sql, QueryReply queryReply) throws SQLException {
    if(Sender.connection.equals(queryRequest.getSender())){
      DatabaseMetaData metaData = connection.getMetaData();
      try {
        Method method = getMethod(metaData, sql);
        Object[] args = (Object[])queryRequest.getParams().getParameters().get(0).getValue();
        Object o = method.invoke(metaData, args);
        SerialResultSet resultSet;
        if (ResultSet.class.isAssignableFrom(method.getReturnType())) {
          resultSet = new SerialResultSet((ResultSet) o);
        } else {
          SerialResultSetMetaData resultSetMetaData = buildResultSetMetaData(method.getReturnType());
          resultSet = new SerialResultSet(resultSetMetaData);
          resultSet.addRows(new SerialRow(1).setValue(0, o));
        }
        queryReply.setRs(resultSet);
      } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
        throw new SQLException(e);
      }
    }else {
      try(PreparedStatement ps = connection.prepareStatement(sql)) {
        ResultSetMetaData metaData = ps.getMetaData();
        //LOG.info("sql= {} ResultSetMetaData={}", sql, metaData);
        if(metaData!=null) {
          SerialResultSetMetaData resultSetMetaData = new SerialResultSetMetaData(metaData);
          queryReply.setRsMeta(resultSetMetaData);
        }
        SerialParameterMetaData parameterMetaData = new SerialParameterMetaData(ps.getParameterMetaData());
        queryReply.setParamMeta(parameterMetaData);
      }
    }
  }

  private SerialResultSetMetaData buildResultSetMetaData(Class<?> returnType) {
    SerialResultSetMetaData resultSetMetaData = new SerialResultSetMetaData();
    if (returnType.equals(Boolean.class)
        || returnType.equals(boolean.class)) {
      resultSetMetaData.addColumn(null, JDBCType.BOOLEAN.getVendorTypeNumber(), 0, 0);
    } else if (returnType.equals(Integer.class)
        || returnType.equals(int.class)) {
      resultSetMetaData.addColumn(null, JDBCType.INTEGER.getVendorTypeNumber(), 10, 0);
    } else if (returnType.equals(Long.class)
        || returnType.equals(long.class)) {
      resultSetMetaData.addColumn(null, JDBCType.BIGINT.getVendorTypeNumber(), 20, 0);
    } else if (returnType.equals(String.class)) {
      resultSetMetaData.addColumn(null, JDBCType.VARCHAR.getVendorTypeNumber(), 0, 0);
    } else {
      resultSetMetaData.addColumn(null, JDBCType.VARCHAR.getVendorTypeNumber(), 0, 0);
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



  @Override
  public Object applyTransaction(TermIndex termIndex, ByteString msg) {

    UpdateReply updateReply = new UpdateReply();
    try {
      UpdateRequest updateRequest = context.as(msg);
      String sessionId = updateRequest.getSession();
      Session session = sessionMgr.getSession(sessionId).orElse(null);

      //LOG.info("type={}, tx={}, sql={}", updateRequest.getType(), updateRequest.getTx(), updateRequest.getSql());
      String tx = updateRequest.getTx();
      if(session!=null){
        session.setTx(tx);
      }
      if(tx.isEmpty()) {
        try (Connection connection = dataSource.getConnection()) {
          executeUpdate(updateRequest, connection, updateReply);
          updateAppliedIndexToMax(termIndex.getIndex());
        } catch (SQLException e) {
          updateReply.setEx(e);
        }
      }else {
        transactionMgr.initializeTx(tx, dataSource::getConnection);
        applyLog(updateRequest, updateReply);
        if (UpdateType.commit.equals(updateRequest.getType())
            ||UpdateType.rollback.equals(updateRequest.getType()))
        {
          //事务完成更新插件事务阶段性索引
          updateAppliedIndexToMax(termIndex.getIndex());
        }else{
          transactionMgr.addIndex(tx, termIndex.getIndex());
        }
      }

    } catch (SQLException e) {
      updateReply.setEx(e);
    }
    return updateReply;
  }




  private void applyLog(UpdateRequest updateRequest, UpdateReply updateReply) throws SQLException {
    String tx = updateRequest.getTx();
    if(UpdateType.execute.equals(updateRequest.getType())){
      try {
        Connection connection = transactionMgr.getConnection(tx);
        executeUpdate(updateRequest, connection, updateReply);
      } catch (SQLException e) {
        updateReply.setEx(e);
      }
    }else if(UpdateType.commit.equals(updateRequest.getType())){
      transactionMgr.commit(tx);
    }else if(UpdateType.rollback.equals(updateRequest.getType())){
      transactionMgr.rollback(tx);
    }else if(UpdateType.savepoint.equals(updateRequest.getType())){
      String name = updateRequest.getSql();
      Savepoint savepoint = transactionMgr.setSavepoint(tx, name);
      updateReply.setSavepoint(JdbcSavepoint.of(savepoint));
    }else if(UpdateType.releaseSavepoint.equals(updateRequest.getType())){
      JdbcSavepoint savepoint = updateRequest.getSavepoint();
      transactionMgr.releaseSavepoint(tx, savepoint);
    }else if(UpdateType.rollbackSavepoint.equals(updateRequest.getType())){
      JdbcSavepoint savepoint = updateRequest.getSavepoint();
      transactionMgr.rollback(tx, savepoint);
    }
  }

  private void executeUpdate(UpdateRequest updateRequest, Connection connection,
                             UpdateReply updateReply) throws SQLException {
    if (Sender.prepared.equals(updateRequest.getSender())
        || Sender.callable.equals(updateRequest.getSender())) {
      update4Prepared(connection, updateRequest, updateReply);
    } else {
      update4Statement(connection, updateRequest, updateReply);
    }
  }

  void update4Prepared(Connection connection,
                       UpdateRequest updateRequest,
                       UpdateReply updateReply) throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement(updateRequest.getSql())) {
      if (!updateRequest.getBatchParams().isEmpty()) {
        for (Parameters batchParam : updateRequest.getBatchParams()) {
          setParams(ps, batchParam);
          ps.addBatch();
        }

        long[] updateCounts = ps.executeLargeBatch();
        for (int i = 0; i < updateCounts.length; i++) {
          updateReply.getCounts().add(updateCounts[i]);
        }
      } else {
        setParams(ps, updateRequest.getParams());
        long updateCount = ps.executeLargeUpdate();
        updateReply.setCount(updateCount);
      }
    }
  }

  private void setParams(PreparedStatement ps, Parameters paramList) throws SQLException {
    if(!paramList.isEmpty()) {
      for (Parameter param : paramList.getParameters()) {
        ps.setObject(param.getIndex(), param.getValue());
      }
    }
  }

  void update4Statement(Connection connection,
                        UpdateRequest updateRequest,
                        UpdateReply updateReply) throws SQLException {
    try (Statement s = connection.createStatement()) {
      if (!updateRequest.getBatchSql().isEmpty()) {
        for (String sql : updateRequest.getBatchSql()) {
          s.addBatch(sql);
        }
        long[] updateCounts = s.executeLargeBatch();
        for (int i = 0; i < updateCounts.length; i++) {
          updateReply.getCounts().add(updateCounts[i]);
        }
      } else {
        long updateCount = s.executeLargeUpdate(updateRequest.getSql());
        updateReply.setCount(updateCount);
      }
    }
  }

  @Override
  public List<FileInfo> takeSnapshot(FileListStateMachineStorage storage, TermIndex last) throws IOException {
    Stopwatch stopwatch = Stopwatch.createStarted();
    final File snapshotFile =  storage.getSnapshotFile(DB2ZIP, last.getTerm(), last.getIndex());
    Path tmpPath = snapshotFile.toPath().resolveSibling(DB + "_tmp");
    if(!tmpPath.toFile().exists()){
      tmpPath.toFile().mkdirs();
    }
    File sqlFile = tmpPath.resolve(db+".sql").toFile();
    try (Connection connection = dataSource.getConnection();
         Statement statement = connection.createStatement()) {
      statement.execute("SCRIPT DROP TO '" + sqlFile.toString() + "'");
    }catch (SQLException e){
      throw new IOException(e);
    }
    ZipUtils.compressFiles(snapshotFile, sqlFile);
    deleteDir(tmpPath);
    LOG.info("Taking a DB snapshot to file {}", snapshotFile);
    final MD5Hash md5 = MD5FileUtil.computeAndSaveMd5ForFile(snapshotFile);
    final FileInfo info = new FileInfo(snapshotFile.toPath(), md5);
    ArrayList<FileInfo> fileInfos = Lists.newArrayList(info);
    LOG.info("Taking a DB snapshot use time: {}", stopwatch);
    return fileInfos;
  }

  private static void deleteDir(Path tmpPath) {
    Arrays.stream(Objects.requireNonNull(tmpPath.toFile().listFiles()))
        .forEach(File::delete);
    tmpPath.toFile().delete();
  }

  @Override
  public void restoreFromSnapshot(SnapshotInfo snapshot) throws IOException {
    if(snapshot==null){
      return;
    }
    List<FileInfo> zipfiles = snapshot.getFiles(DB2ZIP);
    if(!zipfiles.isEmpty()) {

      FileInfo fileInfo = zipfiles.get(0);
      final File snapshotFile = fileInfo.getPath().toFile();
      final String snapshotFileName = snapshotFile.getPath();
      final MD5Hash md5 = MD5FileUtil.computeMd5ForFile(snapshotFile);
      if (md5.equals(fileInfo.getFileDigest())) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        LOG.info("restore DB snapshot from {}", snapshotFileName);
        Path tmpPath = snapshotFile.toPath().resolveSibling(DB + "_tmp");
        File sqlFile = tmpPath.resolve(db+".sql").toFile();
        ZipUtils.decompressFiles(snapshotFile, tmpPath.toFile());
        if(sqlFile.exists()) {
          try (Connection connection = dataSource.getConnection();
               Statement statement = connection.createStatement()) {
            statement.execute("RUNSCRIPT FROM '" + sqlFile.toString() + "'");
          } catch (SQLException e) {
            throw new IOException(e);
          }
        }
        deleteDir(tmpPath);
        LOG.info("restore DB snapshot use time: {}", stopwatch);
      }

    }else{
      List<FileInfo> files = snapshot.getFiles(DB);
      if(!files.isEmpty()) {
        FileInfo fileInfo = files.get(0);
        final File snapshotFile = fileInfo.getPath().toFile();
        final String snapshotFileName = snapshotFile.getPath();

        final MD5Hash md5 = MD5FileUtil.computeMd5ForFile(snapshotFile);
        if (md5.equals(fileInfo.getFileDigest())) {
          Stopwatch stopwatch = Stopwatch.createStarted();
          LOG.info("restore DB snapshot from {}", snapshotFileName);
          try (Connection connection = dataSource.getConnection();
               Statement statement = connection.createStatement()) {
            statement.execute("RUNSCRIPT FROM '" + snapshotFileName + "'");
          } catch (SQLException e) {
            throw new IOException(e);
          }
          LOG.info("restore DB snapshot use time: {}", stopwatch);
        }
      }
    }
  }


  @Override
  public void close() throws IOException {
    try {
      if (dataSource.isClosed()) {
        dataSource.close();
      }
    } catch (SQLException e) {
      LOG.warn("", e);
    }
  }
}
