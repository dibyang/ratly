package net.xdob.ratly.jdbc;

import com.google.common.base.Stopwatch;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.xdob.ratly.io.MD5Hash;
import net.xdob.ratly.jdbc.exception.NoSessionException;
import net.xdob.ratly.jdbc.sql.*;
import net.xdob.ratly.json.Jsons;
import net.xdob.ratly.security.crypto.password.PasswordEncoder;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.raftlog.RaftLogIndex;
import net.xdob.ratly.server.storage.FileInfo;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.statemachine.impl.FileListStateMachineStorage;
import net.xdob.ratly.util.MD5FileUtil;
import net.xdob.ratly.util.N3Map;
import net.xdob.ratly.util.ZipUtils;
import org.h2.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class InnerDb {
  static final Logger LOG = LoggerFactory.getLogger(InnerDb.class);

  public static final String SESSIONS_KEY = "sessions";
  public static final String SQL_EXT = "sql";
  public static final String SESSIONS_JSON_EXT = "sessions.json";
  public static final String DB_EXT = "mv.db";
  public static final String INNER_USER = "remote";
  public static final String INNER_PASSWORD = "hhrhl2016";
  private final HikariConfig dsConfig = new HikariConfig();
  private volatile HikariDataSource dataSource = null;
  private final DefaultSessionMgr sessionMgr;

  private final Path dbStore;
  private final DbInfo dbInfo;

  private final TransactionMgr transactionMgr;

  private final DbsContext context;

  private final RaftLogIndex appliedIndex;

  private final AtomicBoolean initialized = new AtomicBoolean(false);

  private final ClassCache classCache = new ClassCache();

  public InnerDb(Path dbStore, DbInfo dbInfo, DbsContext context) {
    this.dbStore = dbStore;
    this.dbInfo = dbInfo;
    this.context = context;
    appliedIndex = new RaftLogIndex(getName()+"_DbAppliedIndex", RaftLog.INVALID_LOG_INDEX);
    sessionMgr = new DefaultSessionMgr(context);
    transactionMgr = new DefaultTransactionMgr(context);
  }

  public DbInfo getDbInfo() {
    return dbInfo;
  }

  public DbInfo getDbState() {
    return new DbState(dbInfo).setAppliedIndex(getLastPluginAppliedIndex());
  }

  public String getName(){
    return dbInfo.getName();
  }

  public boolean isInitialized() {
    return initialized.get();
  }

  public void initialize() {
    if(initialized.compareAndSet(false, true)){
      /**
       * 初始化数据库连接
       */
      try {
        Driver.load();

        String dbPath = dbStore.resolve(getName()).toString();
        LOG.info("initialize db dbPath={}", dbPath);
        // 基于存储目录初始化
        String url = "jdbc:h2:file:" + dbPath
            + ";AUTO_SERVER=TRUE"
            + ";QUERY_TIMEOUT=600";     // 查询超时设置为 10 分钟（单位：秒）


        dsConfig.setJdbcUrl(url);
        dsConfig.setUsername(INNER_USER);
        dsConfig.setPassword(INNER_PASSWORD);

        // 2. 可选：优化配置
        dsConfig.setConnectionTimeout(30_000);    // 连接超时(ms)
        dsConfig.setIdleTimeout(600_000);         // 空闲超时(ms)
        dsConfig.setMaxLifetime(1_800_000);        // 最大存活时间(ms)
        dsConfig.setMaximumPoolSize(32);         // 最大连接数
        dsConfig.setMinimumIdle(5);              // 最小空闲连接
        dsConfig.addDataSourceProperty("cachePrepStmts", "true"); //


        context.getScheduler().scheduleAtFixedRate(()->{
          transactionMgr.checkTimeoutTx();
        },10, 10, TimeUnit.SECONDS);
        openDs();
//        context.getScheduler().scheduleAtFixedRate(()->{
//          sessionMgr.checkTimeout();
//        },20, 20, TimeUnit.SECONDS);

        restoreFromSnapshot(context.getLatestSnapshot());
      } catch (IOException e) {
        initialized.set(false);
        LOG.warn("initialize failed "+ dbInfo.getName(), e);
      }
    }
  }

  private void openDs() {
    if(dataSource==null){
      dataSource = new HikariDataSource(dsConfig);
    }
  }

  private void closeDs() {
    if(dataSource!=null){
      dataSource.close();
      dataSource = null;
    }
  }


  public void query(QueryRequest queryRequest, QueryReply queryReply) throws SQLException {

    String sessionId = queryRequest.getSession();
    Session session = sessionMgr.getSession(sessionId)
        .orElseThrow(()->new SQLInvalidAuthorizationSpecException(new NoSessionException(sessionId)));
    try {
      query(queryRequest, session, queryReply);
    } finally {
      //没有事务则直接关闭连接
      session.closeConnection();
    }

  }


  private void query(QueryRequest queryRequest, Session session, QueryReply queryReply) throws SQLException {
    if(queryRequest.getType().equals(QueryType.check)){
      doSqlCheck(session, queryRequest.getSql());
    }else if(queryRequest.getType().equals(QueryType.meta)){
      doMeta(session, queryRequest, queryRequest.getSql(), queryReply);
    }else if(queryRequest.getType().equals(QueryType.query)){
      doQuery(session, queryRequest, queryReply);
    }else if(queryRequest.getType().equals(QueryType.invoke)){
      doInvoke(session, queryRequest, queryReply);
    }
  }

  private void doInvoke(Session session, QueryRequest queryRequest,  QueryReply queryReply) throws SQLException {
    if(Sender.databaseMetaData.equals(queryRequest.getSender())){
      DatabaseMetaData metaData = session.getConnection().getMetaData();
      try {
        String methodName  = queryRequest.getMethodName();
        Class<?>[] paramTypes = queryRequest.getParametersTypes();
        Method method = classCache.getMethod(metaData.getClass(), methodName, paramTypes);;
        Object[] args = queryRequest.getArgs();
        Object o = method.invoke(metaData, args);
        ResultSet resultSet;
        if (ResultSet.class.isAssignableFrom(method.getReturnType())) {
          resultSet = new SerialResultSet((ResultSet) o);
        } else {
          SerialResultSetMetaData resultSetMetaData = buildResultSetMetaData(method.getReturnType());
          resultSet = new SerialResultSet(resultSetMetaData)
              .addRows(new SerialRow(1).setValue(0, o));
        }
        queryReply.setRs(resultSet);
      } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
        throw new SQLException(e);
      }
    }

  }

  private void doQuery(Session session, QueryRequest queryRequest,
                       QueryReply queryReply) throws SQLException {
    if(Sender.prepared.equals(queryRequest.getSender())
        ||Sender.callable.equals(queryRequest.getSender())){
      try(CallableStatement ps = session.getConnection().prepareCall(queryRequest.getSql())) {
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
      try(Statement s = session.getConnection().createStatement()) {
        ResultSet rs = s.executeQuery(queryRequest.getSql());
        queryReply.setRs(new SerialResultSet(rs));
      }
    }
  }

  private void doSqlCheck(Session session, String sql) throws SQLException {
    try(PreparedStatement ps = session.getConnection().prepareStatement(sql)) {
      //check sql
    }
  }

  private void doMeta(Session session, QueryRequest queryRequest, String sql, QueryReply queryReply) throws SQLException {
    try(PreparedStatement ps = session.getConnection().prepareStatement(sql)) {
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


  public void applyTransaction(UpdateRequest updateRequest, TermIndex termIndex, UpdateReply updateReply) throws SQLException {

    if(updateRequest.getType()==UpdateType.openSession){
      SessionRequest sessionRequest = SessionRequest.of(updateRequest.getDb(), updateRequest.getUser(), String.valueOf(termIndex.getIndex()));
      String user = sessionRequest.getUser();
      String password = context.getRsaHelper().decrypt(updateRequest.getPassword());
      DbUser dbUser = getDbInfo().getUser(user).orElse(null);
      if (dbUser == null) {
        throw new SQLInvalidAuthorizationSpecException();
      } else {
        PasswordEncoder passwordEncoder = context.getPasswordEncoder();
        if (!passwordEncoder.matches(password, dbUser.getPassword())) {
          throw new SQLInvalidAuthorizationSpecException();
        } else {
          if (passwordEncoder.upgradeEncoding(dbUser.getPassword())) {
            dbUser.setPassword(passwordEncoder.encode(password));
            context.updateDbs();
          }
        }
      }
      openDs();
      Session session = sessionMgr.newSession(sessionRequest, dataSource::getConnection);
      updateReply.setSessionId(session.getId());
    }else if(updateRequest.getType()==UpdateType.closeSession){
      String sessionId = updateRequest.getSession();
      sessionMgr.closeSession(sessionId);
    }else{
      String sessionId = updateRequest.getSession();
      Session session = sessionMgr.getSession(sessionId)
          .orElseThrow(()->new SQLInvalidAuthorizationSpecException(new NoSessionException(sessionId)));

      String tx = updateRequest.getTx();
      session.setTx(tx);
      if(tx.isEmpty()) {
        executeUpdate(updateRequest, session.getConnection(), updateReply);
        updateAppliedIndexToMax(termIndex.getIndex());
        session.closeConnection();
      }else {
        transactionMgr.initializeTx(tx, session);
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
    }
  }


  /**
   * 事务完成更新插件事务阶段性索引
   * @param newIndex 插件事务阶段性索引
   */
  public boolean updateAppliedIndexToMax(long newIndex) {
    return appliedIndex.updateToMax(newIndex,
        message -> LOG.debug("updateAppliedIndex {}", message));
  }

  public long getLastPluginAppliedIndex() {
    return appliedIndex.get();
  }


  private void applyLog(UpdateRequest updateRequest, UpdateReply updateReply) throws SQLException {
    String tx = updateRequest.getTx();
    if(UpdateType.execute.equals(updateRequest.getType())){
      try {
        Connection connection = transactionMgr.getSession(tx).getConnection();
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
      ps.setQueryTimeout(60);
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
      s.setQueryTimeout(60);
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

  private void takeSqlSnapshot(FileListStateMachineStorage storage, TermIndex last, FileInfo dbFileInfo){
    File sqlFile = storage.getSnapshotFile(getName() + "." +SQL_EXT, last.getTerm(), last.getIndex());
    File dbPath = storage.getSnapshotFile(getName(), last.getTerm(), last.getIndex());
    Stopwatch stopwatch = Stopwatch.createStarted();
    // 基于存储目录初始化
    String url = "jdbc:h2:file:" + dbPath.toString()
        + ";AUTO_SERVER=TRUE"
        + ";QUERY_TIMEOUT=600";
    try (Connection connection = DriverManager.getConnection(url, INNER_USER, INNER_PASSWORD);
         Statement statement = connection.createStatement()) {
      statement.setQueryTimeout(600);
      statement.execute("SCRIPT DROP TO '" + sqlFile.toString() + "'");
    }catch (SQLException e){
      LOG.warn("takeSqlSnapshot error", e);
    }
    MD5FileUtil.computeAndSaveMd5ForFile(sqlFile);
    MD5Hash md5Hash = MD5FileUtil.computeAndSaveMd5ForFile(dbFileInfo.getPath().toFile());
    dbFileInfo.setFileDigest(md5Hash);
    LOG.info("takeSqlSnapshot to file {}, use time:{}", sqlFile.toString(), stopwatch);

  }

  public List<FileInfo> takeSnapshot(FileListStateMachineStorage storage, TermIndex last) throws IOException {
    List<FileInfo> infos = new ArrayList<>();
    Stopwatch stopwatch = Stopwatch.createStarted();

    try(Connection connection = dataSource.getConnection();
        Statement stmt = connection.createStatement()){
      //快照前让数据落盘  CHECKPOINT SYNC
      stmt.executeUpdate("checkpoint sync");
    }catch (SQLException e){
      throw new IOException(e);
    }
    File dbFile = storage.getSnapshotFile(getName() + "." +DB_EXT, last.getTerm(), last.getIndex());
    Path sourceDbFile = dbStore.resolve(getName()+ "." +DB_EXT);
    Files.copy(sourceDbFile, dbFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    LOG.info("Taking a DB snapshot to file {}, use time:{}", dbFile.toString(), stopwatch);
    FileInfo dbFileInfo = getFileInfo(dbFile);
    infos.add(dbFileInfo);
    stopwatch.reset().start();
    context.getScheduler().submit(()->{
      takeSqlSnapshot(storage, last, dbFileInfo);
    });
    File sessionFile = storage.getSnapshotFile(getName() + "." +SESSIONS_JSON_EXT, last.getTerm(), last.getIndex());
    List<String> sessionIds = sessionMgr.getAllSessions().stream().map(Session::getId)
        .sorted(Comparator.comparing(e->e))
        .collect(Collectors.toList());
    N3Map n3Map = new N3Map();
    n3Map.put(SESSIONS_KEY, sessionIds);
    String json = Jsons.i.toJson(n3Map);
    Files.write(sessionFile.toPath(), json.getBytes(StandardCharsets.UTF_8));
    LOG.info("Taking a DB snapshot to file {}, use time:{}", sessionFile.toString(), stopwatch);

    infos.add(getFileInfo(sessionFile));

    return infos;
  }

  private static FileInfo getFileInfo(File file) {
    final MD5Hash md5 = MD5FileUtil.computeAndSaveMd5ForFile(file);
		return new FileInfo(file.toPath(), md5);
  }


  public void restoreFromSnapshot(SnapshotInfo snapshot) throws IOException {
    if(snapshot==null){
      return;
    }
    Stopwatch stopwatch = Stopwatch.createStarted();
    boolean restoreDb = false;
    FileInfo dbfileInfo = snapshot.getFiles(getName() + "." + DB_EXT).stream().findFirst().orElse(null);
    if(dbfileInfo!=null) {
      final File dbFile = dbfileInfo.getPath().toFile();
      final MD5Hash md5 = MD5FileUtil.computeMd5ForFile(dbFile);
      if (md5.equals(dbfileInfo.getFileDigest())) {
        LOG.info("restore DB file snapshot from {}", dbFile.getPath());
        closeDs();
        Files.copy(dbFile.toPath(), dbStore.resolve(getName()+ "." + DB_EXT), StandardCopyOption.REPLACE_EXISTING);
        openDs();
        restoreDb = true;
      }else{
        LOG.warn("DB file snapshot md5 mismatch, expected {}, actual {}", dbfileInfo.getFileDigest(), md5);
      }
    }
    if(!restoreDb){
      FileInfo sqlfileInfo = snapshot.getFiles(getName() + "." + SQL_EXT).stream().findFirst().orElse(null);
      if(sqlfileInfo!=null) {
        final File sqlFile = sqlfileInfo.getPath().toFile();
        final MD5Hash md5 = MD5FileUtil.computeMd5ForFile(sqlFile);
        if (md5.equals(sqlfileInfo.getFileDigest())) {
          LOG.info("restore DB sql snapshot from {}", sqlFile.getPath());
          try (Connection connection = dataSource.getConnection();
               Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(600);
            statement.execute("RUNSCRIPT FROM '" + sqlFile.toString() + "'");
          } catch (SQLException e) {
            throw new IOException(e);
          }
        }else {
          LOG.warn("DB sql snapshot md5 mismatch, expected {}, actual {}", dbfileInfo.getFileDigest(), md5);
        }
      }
    }

    FileInfo sessionfileInfo = snapshot.getFiles(getName() + "." + SESSIONS_JSON_EXT).stream().findFirst().orElse(null);
    if(sessionfileInfo!=null) {
      final File sessionFile = sessionfileInfo.getPath().toFile();
      final MD5Hash md5 = MD5FileUtil.computeMd5ForFile(sessionFile);
      if (md5.equals(sessionfileInfo.getFileDigest())) {
        byte[] bytes = Files.readAllBytes(sessionFile.toPath());
        N3Map n3Map = Jsons.i.fromJson(new String(bytes, StandardCharsets.UTF_8), N3Map.class);
        List<String> sessionIds = n3Map.getStrings(SESSIONS_KEY);
        for (String sessionId : sessionIds) {
          try {
            sessionMgr.newSession(SessionRequest.fromSessionId(dbInfo.getName(), sessionId), dataSource::getConnection);
          } catch (SQLException e) {
            LOG.warn("node {} newSession error.", context.getPeerId(), e);
          }
        }
      }
    }
    LOG.info("restore DB snapshot use time: {}", stopwatch);
  }


  public void close() throws IOException {
    if (!dataSource.isClosed()) {
      dataSource.close();
    }
  }
}
