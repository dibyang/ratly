package net.xdob.ratly.jdbc;

import com.google.common.base.Stopwatch;
import net.xdob.ratly.io.MD5Hash;
import net.xdob.ratly.jdbc.exception.NoSessionException;
import net.xdob.ratly.jdbc.sql.*;
import net.xdob.ratly.security.crypto.password.PasswordEncoder;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.raftlog.RaftLogIndex;
import net.xdob.ratly.server.storage.FileInfo;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.statemachine.impl.FileListStateMachineStorage;
import net.xdob.ratly.util.MD5FileUtil;
import net.xdob.ratly.util.ZipUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.h2.Driver;
import org.h2.util.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class InnerDb {
  static final Logger LOG = LoggerFactory.getLogger(InnerDb.class);
  public static final String DB2ZIP = "db.zip";
  public static final String INNER_USER = "remote";
  public static final String INNER_PASSWORD = "hhrhl2016";
  private final BasicDataSource dataSource = new BasicDataSource();
  private final DefaultSessionMgr sessionMgr = new DefaultSessionMgr();

  private final Path dbStore;
  private final DbInfo dbInfo;

  private final TransactionMgr transactionMgr = new DefaultTransactionMgr();

  private final DbsContext context;

  private final RaftLogIndex appliedIndex;

  private final AtomicBoolean initialized = new AtomicBoolean(false);

  private final ClassCache classCache = new ClassCache();

  public InnerDb(Path dbStore, DbInfo dbInfo, DbsContext context) {
    this.dbStore = dbStore;
    this.dbInfo = dbInfo;
    this.context = context;
    appliedIndex = new RaftLogIndex(getName()+"_DbAppliedIndex", RaftLog.INVALID_LOG_INDEX);
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
        File dbFile = dbStore.resolve(getName() + ".mv.db").toFile();
        if(dbFile.exists()){
          dbFile.delete();
        }
        String dbPath = dbStore.resolve(getName()).toString();
        // 基于存储目录初始化
        String url = "jdbc:h2:file:" + dbPath;
        LOG.info("initialize db dbPath={}", dbPath);
        dataSource.setUrl(url + ";AUTO_SERVER=TRUE");
        dataSource.setDriverClassName("org.h2.Driver");
        dataSource.setUsername(INNER_USER);
        dataSource.setPassword(INNER_PASSWORD);
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


        context.getScheduler().scheduleAtFixedRate(()->{
          transactionMgr.checkTimeoutTx();
        },10, 10, TimeUnit.SECONDS);

        context.getScheduler().scheduleAtFixedRate(()->{
          sessionMgr.checkTimeout();
        },20, 20, TimeUnit.SECONDS);


        restoreFromSnapshot(context.getLatestSnapshot());
      } catch (IOException e) {
        initialized.set(false);
        LOG.warn("initialize failed "+ dbInfo.getName(), e);
      }
    }
  }


  public void query(QueryRequest queryRequest, QueryReply queryReply) throws SQLException {

    Sender sender = queryRequest.getSender();
    QueryType type = queryRequest.getType();
    if(Sender.connection.equals(sender)&&QueryType.check.equals(type)){
      String user = queryRequest.getUser();
      String password = context.getRsaHelper().decrypt(queryRequest.getPassword());
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
      Session session = sessionMgr.newSession(user, dataSource::getConnection);
      queryReply.setRs(session.getId());

    }else{
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

    String sessionId = updateRequest.getSession();
    Session session = sessionMgr.getOrOpenSession(sessionId, dataSource::getConnection);

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


  /**
   * 事务完成更新插件事务阶段性索引
   * @param newIndex 插件事务阶段性索引
   */
  public boolean updateAppliedIndexToMax(long newIndex) {
    return appliedIndex.updateToMax(newIndex,
        message -> LOG.debug("updateAppliedIndex {}", message));
  }

  public long getLastPluginAppliedIndex() {
    if(!transactionMgr.isTransaction()){
      return RaftLog.INVALID_LOG_INDEX;
    }
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

  public List<FileInfo> takeSnapshot(FileListStateMachineStorage storage, TermIndex last) throws IOException {

    String module = getName() + "." + DB2ZIP;
    File snapshotFile = storage.getSnapshotFile(module, last.getTerm(), last.getIndex());

    File sqlFile = snapshotFile.toPath().resolveSibling(getName() + ".sql").toFile();
    Stopwatch stopwatch = Stopwatch.createStarted();
    try (Connection connection = dataSource.getConnection();
         Statement statement = connection.createStatement()) {
      statement.execute("SCRIPT DROP TO '" + sqlFile.toString() + "'");
    }catch (SQLException e){
      throw new IOException(e);
    }
    ZipUtils.compressFiles(snapshotFile, sqlFile);
    sqlFile.delete();
    LOG.info("Taking a DB snapshot to file {}, use time:", snapshotFile.toString(), stopwatch);
    List<FileInfo> infos = new ArrayList<>();
    final MD5Hash md5 = MD5FileUtil.computeAndSaveMd5ForFile(snapshotFile);
    final FileInfo info = new FileInfo(snapshotFile.toPath(), md5);
    infos.add(info);
    return infos;
  }


  public void restoreFromSnapshot(SnapshotInfo snapshot) throws IOException {
    if(snapshot==null){
      return;
    }

    String module = getName() + "." + DB2ZIP;
    List<FileInfo> zipfiles = snapshot.getFiles(module);
    if(!zipfiles.isEmpty()) {
      FileInfo fileInfo = zipfiles.get(0);
      final File snapshotFile = fileInfo.getPath().toFile();
      final String snapshotFileName = snapshotFile.getPath();
      final MD5Hash md5 = MD5FileUtil.computeMd5ForFile(snapshotFile);
      if (md5.equals(fileInfo.getFileDigest())) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        LOG.info("restore DB snapshot from {}", snapshotFileName);
        ZipUtils.decompressFiles(snapshotFile, snapshotFile.getParentFile());
        File sqlFile = snapshotFile.toPath().resolveSibling(getName() + ".sql").toFile();
        if(sqlFile.exists()) {
          try (Connection connection = dataSource.getConnection();
               Statement statement = connection.createStatement()) {
            statement.execute("RUNSCRIPT FROM '" + sqlFile.toString() + "'");
          } catch (SQLException e) {
            throw new IOException(e);
          }
          sqlFile.delete();
        }
        LOG.info("restore DB snapshot use time: {}", stopwatch);
      }
    }
  }


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
