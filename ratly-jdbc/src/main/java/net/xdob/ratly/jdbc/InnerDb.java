package net.xdob.ratly.jdbc;

import com.google.common.base.Stopwatch;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.xdob.ratly.io.Digest;
import net.xdob.ratly.jdbc.exception.NoSessionException;
import net.xdob.ratly.jdbc.proto.JdbcValue;
import net.xdob.ratly.jdbc.proto.SqlExConverter;
import net.xdob.ratly.jdbc.sql.*;
import net.xdob.ratly.json.Jsons;
import net.xdob.ratly.proto.jdbc.*;
import net.xdob.ratly.security.crypto.password.PasswordEncoder;
import net.xdob.ratly.server.exception.DbErrorException;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.raftlog.RaftLogIndex;
import net.xdob.ratly.server.storage.FileInfo;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.statemachine.impl.FileListStateMachineStorage;
import net.xdob.ratly.util.MD5FileUtil;
import net.xdob.ratly.util.N3Map;
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

  public int maxPoolSize = 56;

  public InnerDb(Path dbStore, DbInfo dbInfo, DbsContext context) {
    this.dbStore = dbStore;
    this.dbInfo = dbInfo;
    this.context = context;
    appliedIndex = new RaftLogIndex(getName()+"_DbAppliedIndex", RaftLog.INVALID_LOG_INDEX);
    sessionMgr = new DefaultSessionMgr(context).setMaxSessions(maxPoolSize-8);
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

        dsConfig.setPoolName(this.context.getPeerId()+"$"+getName());
        dsConfig.setJdbcUrl(url);
        dsConfig.setUsername(INNER_USER);
        dsConfig.setPassword(INNER_PASSWORD);

        // 2. 可选：优化配置
        dsConfig.setConnectionTimeout(30_000);    // 连接超时(ms)
        dsConfig.setIdleTimeout(600_000);         // 空闲超时(ms)
        dsConfig.setMaxLifetime(1_800_000);        // 最大存活时间(ms)
        dsConfig.setMaximumPoolSize(maxPoolSize);         // 最大连接数
        dsConfig.setMinimumIdle(5);              // 最小空闲连接
        dsConfig.addDataSourceProperty("cachePrepStmts", "true"); //
        dsConfig.setRegisterMbeans(true);
        openDs();
        context.getScheduler()
            .scheduleWithFixedDelay(transactionMgr::checkTimeoutTx,
                10, 10, TimeUnit.SECONDS);
        context.getScheduler()
            .scheduleWithFixedDelay(sessionMgr::checkExpiredSessions,
                20, 20, TimeUnit.SECONDS);

        restoreFromSnapshot(context.getLatestSnapshot());
      } catch (IOException e) {
        initialized.set(false);
        LOG.warn("initialize failed "+ dbInfo.getName(), e);
      }
    }
  }

  private void openDs() {
    if(dataSource == null){
      dataSource = new HikariDataSource(dsConfig);
    }
    LOG.info("open db ds {} for {}", dataSource, getName());
  }

  private void closeDs() {
    if(dataSource!=null){
      LOG.info("close db ds {} for {}", dataSource, getName());
      dataSource.close();
      dataSource = null;
    }
  }


  public void query(JdbcRequestProto queryRequest, JdbcResponseProto.Builder response) throws SQLException {

    String sessionId = queryRequest.getSessionId();
    Session session = sessionMgr.getSession(sessionId)
        .orElseThrow(()->new SQLInvalidAuthorizationSpecException(new NoSessionException(sessionId)));
    try {
      query(queryRequest, session, response);
    } finally {
      //没有事务则直接关闭连接
      session.closeConnection();
    }

  }


  private void query(JdbcRequestProto queryRequest, Session session, JdbcResponseProto.Builder response) throws SQLException {
    if(queryRequest.hasConnRequest()){
      databaseMetaData(session, queryRequest, response);
    }else{
      SqlRequestProto sqlRequest = queryRequest.getSqlRequest();
      if(sqlRequest.getType().equals(SqlRequestType.parameterMeta)){
        parameterMeta(session, queryRequest, response);
      }else if(sqlRequest.getType().equals(SqlRequestType.resultSetMetaData)){
        resultSetMetaData(session, queryRequest, response);
      } if(sqlRequest.getType().equals(SqlRequestType.query)){
        doQuery(session, queryRequest, response);
      }
    }
  }


  private void databaseMetaData(Session session, JdbcRequestProto queryRequest,  JdbcResponseProto.Builder response) throws SQLException {
    DatabaseMetaData metaData = session.getConnection().getMetaData();
    try {
      DatabaseMetaRequestProto databaseMetaRequest = queryRequest.getConnRequest().getDatabaseMetaRequest();
      String methodName  = databaseMetaRequest.getMethod();
      Class<?>[] paramTypes = new Class[databaseMetaRequest.getParametersTypesCount()];
      for (int i = 0; i < databaseMetaRequest.getParametersTypesList().size(); i++) {
        String parametersType = databaseMetaRequest.getParametersTypes(i);
        paramTypes[i] = convertType(parametersType);
      }

      Method method = classCache.getMethod(metaData.getClass(), methodName, paramTypes);;
      Object[] args = new Object[databaseMetaRequest.getArgsCount()];
      for (int i = 0; i < databaseMetaRequest.getArgsList().size(); i++) {
        args[i] = JdbcValue.toJavaObject(databaseMetaRequest.getArgs(i));
      }
      Object o = method.invoke(metaData, args);
      ResultSet resultSet;
      if (ResultSet.class.isAssignableFrom(method.getReturnType())) {
        resultSet = new SerialResultSet((ResultSet) o);
      } else {
        SerialResultSetMetaData resultSetMetaData = buildResultSetMetaData(method.getReturnType());
        resultSet = new SerialResultSet(resultSetMetaData)
            .addRows(new SerialRow(1).setValue(0, o));
      }
      response.setResultSet(SerialResultSet.toProto(resultSet));
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException | ClassNotFoundException e) {
      throw new SQLException(e);
    }

  }

  private void parameterMeta(Session session, JdbcRequestProto queryRequest,  JdbcResponseProto.Builder response) throws SQLException {
    SqlRequestProto sqlRequest = queryRequest.getSqlRequest();
    try(PreparedStatement ps = session.getConnection().prepareStatement(sqlRequest.getSql())) {
      SerialParameterMetaData parameterMetaData = new SerialParameterMetaData(ps.getParameterMetaData());
      response.setParameterMeta(ParameterMetaProto.newBuilder()
          .addAllParameters(parameterMetaData.toProto()));
    }
  }

  private void resultSetMetaData(Session session, JdbcRequestProto queryRequest,  JdbcResponseProto.Builder response) throws SQLException {
    SqlRequestProto sqlRequest = queryRequest.getSqlRequest();
    try(PreparedStatement ps = session.getConnection().prepareStatement(sqlRequest.getSql())) {
      SerialResultSetMetaData resultSetMetaData = new SerialResultSetMetaData(ps.getMetaData());
      response.setResultSet(ResultSetProto.newBuilder()
          .addAllColumns(resultSetMetaData.toProto()));
    }
  }


  private Class<?> convertType(String parametersType) throws ClassNotFoundException {
    Class<?> type = null;
    switch (parametersType) {
      case "int":
        type = int.class;
        break;
      case "long":
        type = long.class;
        break;
      case "double":
        type = double.class;
        break;
      case "boolean":
        type = boolean.class;
        break;
      case "String":
        type = String.class;
        break;
      default:
        type = Class.forName(parametersType);
    }
    return type;
  }

  private void doQuery(Session session, JdbcRequestProto queryRequest,  JdbcResponseProto.Builder response) throws SQLException {
    SqlRequestProto sqlRequest = queryRequest.getSqlRequest();
    if(StmtType.prepared.equals(sqlRequest.getStmtType())
        ||StmtType.callable.equals(sqlRequest.getStmtType())){
      try(CallableStatement ps = session.getConnection().prepareCall(sqlRequest.getSql())) {
        if(sqlRequest.hasFetchDirection()){
          if(sqlRequest.getFetchDirection()== ResultSet.FETCH_FORWARD){
            ps.setFetchDirection(ResultSet.FETCH_FORWARD);
          }else if(sqlRequest.getFetchDirection()== ResultSet.FETCH_REVERSE){
            ps.setFetchDirection(ResultSet.FETCH_REVERSE);
          }else if(sqlRequest.getFetchDirection()== ResultSet.FETCH_UNKNOWN){
            ps.setFetchDirection(ResultSet.FETCH_UNKNOWN);
          }
        }
        if(sqlRequest.hasFetchSize()) {
          ps.setFetchSize(sqlRequest.getFetchSize());
        }
        List<ParameterProto> paramList = sqlRequest.getParams().getParamList();
        if(!paramList.isEmpty()){
          for (ParameterProto parameterProto : paramList) {
            Parameter parameter = Parameter.from(parameterProto);
            Object value = parameter.getValue();
            ps.setObject(parameter.getIndex(), value);
          }
        }
        ResultSet rs = ps.executeQuery();
        response.setResultSet(new SerialResultSet(rs).toProto());
      }
    }else{
      try(Statement s = session.getConnection().createStatement()) {
        ResultSet rs = s.executeQuery(sqlRequest.getSql());
        response.setResultSet(new SerialResultSet(rs).toProto());
      }
    }
  }

  private SerialResultSetMetaData buildResultSetMetaData(Class<?> returnType) {
    SerialResultSetMetaData resultSetMetaData = new SerialResultSetMetaData();
    if (returnType.equals(Boolean.class)
        || returnType.equals(boolean.class)) {
      resultSetMetaData.addColumn("val", JDBCType.BOOLEAN.getVendorTypeNumber(), 0, 0);
    } else if (returnType.equals(Integer.class)
        || returnType.equals(int.class)) {
      resultSetMetaData.addColumn("val", JDBCType.INTEGER.getVendorTypeNumber(), 10, 0);
    } else if (returnType.equals(Long.class)
        || returnType.equals(long.class)) {
      resultSetMetaData.addColumn("val", JDBCType.BIGINT.getVendorTypeNumber(), 20, 0);
    } else if (returnType.equals(String.class)) {
      resultSetMetaData.addColumn("val", JDBCType.VARCHAR.getVendorTypeNumber(), 0, 0);
    } else {
      resultSetMetaData.addColumn("val", JDBCType.VARCHAR.getVendorTypeNumber(), 0, 0);
    }
    return resultSetMetaData;
  }


  public void applyTransaction(TermIndex termIndex, JdbcRequestProto request, JdbcResponseProto.Builder response) throws SQLException {
    if(request.hasConnRequest()){
      ConnRequestProto connRequest = request.getConnRequest();
      if(connRequest.getType()==ConnRequestType.openSession){
        OpenSessionProto openSession = request.getOpenSession();
        SessionRequest sessionRequest = SessionRequest.of(request.getDb(), openSession.getUser(), termIndex.getIndex());
        String user = sessionRequest.getUser();
        String password = context.getRsaHelper().decrypt(openSession.getPassword());
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
        Session session = sessionMgr.newSession(sessionRequest, this::getConnection);
        response.setSessionId(session.getId());
      }else if(connRequest.getType()==ConnRequestType.closeSession){
        String sessionId = request.getSessionId();
        sessionMgr.closeSession(sessionId);
      }else{
        String sessionId = request.getSessionId();
        Session session = sessionMgr.getSession(sessionId)
            .orElseThrow(()->new SQLInvalidAuthorizationSpecException(new NoSessionException(sessionId)));
        String tx = connRequest.getTx();
        transactionMgr.initializeTx(tx, session);
        applyLog4Conn(connRequest, response);
        if (ConnRequestType.commit.equals(connRequest.getType())
            ||ConnRequestType.rollback.equals(connRequest.getType()))
        {
          //事务完成更新插件事务阶段性索引
          updateAppliedIndexToMax(termIndex.getIndex());
        }else{
          transactionMgr.addIndex(tx, termIndex.getIndex());
        }
      }
    }else if(request.hasSqlRequest()){
      String sessionId = request.getSessionId();
      Session session = sessionMgr.getSession(sessionId)
          .orElseThrow(()->new SQLInvalidAuthorizationSpecException(new NoSessionException(sessionId)));
      SqlRequestProto sqlRequest = request.getSqlRequest();
      String tx = sqlRequest.getTx();
      session.setTx(tx);
      if(tx.isEmpty()) {
        executeUpdate(sqlRequest, session.getConnection(), response);
        updateAppliedIndexToMax(termIndex.getIndex());
        session.closeConnection();
      }else {
        transactionMgr.initializeTx(tx, session);
        applyLog4Sql(sqlRequest, response);
        transactionMgr.addIndex(tx, termIndex.getIndex());
      }
    }

  }

  private Connection getConnection() throws SQLException {
    LOG.info("get connection from {}", dataSource);
    return dataSource.getConnection();
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


  private void applyLog4Conn(ConnRequestProto connRequest, JdbcResponseProto.Builder response) throws SQLException {

    String tx = connRequest.getTx();
    if(ConnRequestType.commit.equals(connRequest.getType())){
      transactionMgr.commit(tx);
    }else if(ConnRequestType.rollback.equals(connRequest.getType())){
      transactionMgr.rollback(tx);
    }else if(ConnRequestType.savepoint.equals(connRequest.getType())){
      Savepoint s = JdbcSavepoint.from(connRequest.getSavepoint());
      Savepoint savepoint = transactionMgr.setSavepoint(tx, s.getSavepointName());
      response.setSavepoint(JdbcSavepoint.of(savepoint).toProto());
    }else if(ConnRequestType.releaseSavepoint.equals(connRequest.getType())){
      Savepoint savepoint = JdbcSavepoint.from(connRequest.getSavepoint());
      transactionMgr.releaseSavepoint(tx, savepoint);
    }else if(ConnRequestType.rollbackSavepoint.equals(connRequest.getType())){
      Savepoint savepoint = JdbcSavepoint.from(connRequest.getSavepoint());
      transactionMgr.rollback(tx, savepoint);
    }
  }

  private void applyLog4Sql(SqlRequestProto sqlRequest, JdbcResponseProto.Builder response) throws SQLException {

    String tx = sqlRequest.getTx();
    try {
      Connection connection = transactionMgr.getSession(tx).getConnection();
      executeUpdate(sqlRequest, connection, response);
    } catch (SQLException e) {
      response.setEx(SqlExConverter.toProto(e));
    }
  }

  private void executeUpdate(SqlRequestProto request, Connection connection,
                             JdbcResponseProto.Builder response) throws SQLException {
    if (StmtType.prepared.equals(request.getStmtType())
        || StmtType.callable.equals(request.getStmtType())) {
      update4Prepared(connection, request, response);
    } else {
      update4Statement(connection, request, response);
    }
  }

  void update4Prepared(Connection connection,
                       SqlRequestProto sqlRequest,
                       JdbcResponseProto.Builder response) throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement(sqlRequest.getSql())) {
      ps.setQueryTimeout(60);
      if (!sqlRequest.getBatchParamsList().isEmpty()) {
        for (ParametersProto parametersProto : sqlRequest.getBatchParamsList()) {
          setParams(ps, Parameters.from(parametersProto));
          ps.addBatch();
        }

        long[] updateCounts = ps.executeLargeBatch();
        UpdateCounts.Builder builder = UpdateCounts.newBuilder();
				for (long updateCount : updateCounts) {
					builder.addUpdateCount(updateCount);
				}
        response.setUpdateCounts(builder);
      } else {
        setParams(ps, Parameters.from(sqlRequest.getParams()));
        long updateCount = ps.executeLargeUpdate();
        response.setUpdateCount(updateCount);
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
                        SqlRequestProto sqlRequest,
                        JdbcResponseProto.Builder response) throws SQLException {
    try (Statement s = connection.createStatement()) {
      s.setQueryTimeout(60);
      if (!sqlRequest.getBatchSqlList().isEmpty()) {
        for (String sql : sqlRequest.getBatchSqlList()) {
          s.addBatch(sql);
        }
        long[] updateCounts = s.executeLargeBatch();
        UpdateCounts.Builder builder = UpdateCounts.newBuilder();
        for (long updateCount : updateCounts) {
          builder.addUpdateCount(updateCount);
        }
        response.setUpdateCounts(builder);
      } else {
        long updateCount = s.executeLargeUpdate(sqlRequest.getSql());
        response.setUpdateCount(updateCount);
      }
    }
  }

  private void takeSqlSnapshot(FileListStateMachineStorage storage, TermIndex last, FileInfo dbFileInfo, FileInfo sqlFileInfo){
    File sqlFile = sqlFileInfo.getPath().toFile();
    File dbPath = storage.getSnapshotFile(getName(), last.getTerm(), last.getIndex());
    Stopwatch stopwatch = Stopwatch.createStarted();
    // 基于存储目录初始化
    String url = "jdbc:h2:file:" + dbPath.toString()
        + ";QUERY_TIMEOUT=600";
    try (Connection connection = DriverManager.getConnection(url, INNER_USER, INNER_PASSWORD);
         Statement statement = connection.createStatement()) {
      statement.setQueryTimeout(600);
      statement.execute("SCRIPT DROP TO '" + sqlFile.toString() + "'");
    }catch (SQLException e){
      LOG.warn("takeSqlSnapshot error", e);
    }
    Digest digest = MD5FileUtil.computeAndSaveDigestForFile(sqlFile);
    sqlFileInfo.setFileDigest(digest);
    Digest digest2 = MD5FileUtil.computeAndSaveDigestForFile(dbFileInfo.getPath().toFile());
    dbFileInfo.setFileDigest(digest2);
    LOG.info("takeSqlSnapshot to file {}, use time:{}", sqlFile.toString(), stopwatch);
  }

  public List<FileInfo> takeSnapshot(FileListStateMachineStorage storage, TermIndex last) throws IOException {
    List<FileInfo> infos = new ArrayList<>();
    Stopwatch stopwatch = Stopwatch.createStarted();
    try(Connection connection = getConnection();
        Statement stmt = connection.createStatement()){
      stmt.setQueryTimeout(600);
      //快照前让数据落盘  CHECKPOINT SYNC
      stmt.executeUpdate("checkpoint sync");
    }catch (SQLException e){
      throw new IOException(e);
    }

    File dbFile = storage.getSnapshotFile(getName() + "." +DB_EXT, last.getTerm(), last.getIndex());
    Path sourceDbFile = dbStore.resolve(getName()+ "." +DB_EXT);
    if(!sourceDbFile.toFile().exists()){
      throw new IOException(DbErrorException.notExists(sourceDbFile.toString()));
    }
    if(sourceDbFile.toFile().length()<128){
      throw new IOException(DbErrorException.error(sourceDbFile.toString()));
    }
    Files.copy(sourceDbFile, dbFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

    FileInfo dbFileInfo = new FileInfo(dbFile.toPath(), null);
    infos.add(dbFileInfo);
//    生成sql快照
    File sqlFile = storage.getSnapshotFile(getName() + "." +SQL_EXT, last.getTerm(), last.getIndex());
    FileInfo sqlFileInfo = new FileInfo(sqlFile.toPath(), null);
    infos.add(sqlFileInfo);
    context.getScheduler().submit(()->{
      takeSqlSnapshot(storage, last, dbFileInfo, sqlFileInfo);
    });
    File sessionFile = storage.getSnapshotFile(getName() + "." +SESSIONS_JSON_EXT, last.getTerm(), last.getIndex());
    List<String> sessionIds = sessionMgr.getAllSessions().stream().map(Session::getId)
        .sorted(Comparator.comparing(e->e))
        .collect(Collectors.toList());
    N3Map n3Map = new N3Map();
    n3Map.put(SESSIONS_KEY, sessionIds);
    String json = Jsons.i.toJson(n3Map);
    Files.write(sessionFile.toPath(), json.getBytes(StandardCharsets.UTF_8));
    infos.add(getFileInfo(sessionFile));
    LOG.info("{} Taking a DB snapshot, use time:{}", getName(), stopwatch);
    return infos;
  }


  private FileInfo getFileInfo(File file) {
    final Digest digest = MD5FileUtil.computeAndSaveDigestForFile(file);
		return new FileInfo(file.toPath(), digest);
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
      final Digest digest = MD5FileUtil.computeDigestForFile(dbFile);
      if (digest.equals(dbfileInfo.getFileDigest())) {
        LOG.info("restore DB file snapshot from {}", dbFile.getPath());
        try {
          closeDs();
          Files.copy(dbFile.toPath(), dbStore.resolve(getName() + "." + DB_EXT), StandardCopyOption.REPLACE_EXISTING);
        }finally {
          openDs();
        }
        restoreDb = true;
      }else{
        LOG.warn("DB file snapshot digest mismatch, expected {}, actual {}", dbfileInfo.getFileDigest(), digest);
      }
    }

    if(!restoreDb){
      FileInfo sqlfileInfo = snapshot.getFiles(getName() + "." + SQL_EXT).stream().findFirst().orElse(null);
      if(sqlfileInfo!=null) {
        final File sqlFile = sqlfileInfo.getPath().toFile();
        final Digest digest = MD5FileUtil.computeDigestForFile(sqlFile);
        if (digest.equals(sqlfileInfo.getFileDigest())) {
          LOG.info("restore DB sql snapshot from {}", sqlFile.getPath());
          try (Connection connection = getConnection();
               Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(600);
            statement.execute("RUNSCRIPT FROM '" + sqlFile.toString() + "'");
          } catch (SQLException e) {
            throw new IOException(e);
          }
        }else {
          LOG.warn("DB sql snapshot digest mismatch, expected {}, actual {}", dbfileInfo.getFileDigest(), digest);
        }
      }
    }
    sessionMgr.clearSessions();
    FileInfo sessionfileInfo = snapshot.getFiles(getName() + "." + SESSIONS_JSON_EXT).stream().findFirst().orElse(null);
    if(sessionfileInfo!=null) {
      final File sessionFile = sessionfileInfo.getPath().toFile();
      final Digest digest = MD5FileUtil.computeDigestForFile(sessionFile);
      if (digest.equals(sessionfileInfo.getFileDigest())) {
        byte[] bytes = Files.readAllBytes(sessionFile.toPath());
        N3Map n3Map = Jsons.i.fromJson(new String(bytes, StandardCharsets.UTF_8), N3Map.class);
        List<String> sessionIds = n3Map.getStrings(SESSIONS_KEY);
        for (String sessionId : sessionIds) {
          try {
            sessionMgr.newSession(SessionRequest.fromSessionId(dbInfo.getName(), sessionId), this::getConnection);
          } catch (SQLException e) {
            LOG.warn("node {} newSession error.", context.getPeerId(), e);
          }
        }
      }
    }
    LOG.info("{} restore DB snapshot use time: {}", getName(), stopwatch);
  }


  public void close() {
    closeDs();

  }


}
