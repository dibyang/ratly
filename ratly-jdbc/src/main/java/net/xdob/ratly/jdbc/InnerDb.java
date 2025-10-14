package net.xdob.ratly.jdbc;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLBeginStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowSessionStatement;
import com.google.common.base.Stopwatch;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.xdob.ratly.jdbc.exception.AuthorizationFailedException;
import net.xdob.ratly.jdbc.exception.TooManySessionException;
import net.xdob.ratly.jdbc.sql.*;
import net.xdob.ratly.io.Digest;
import net.xdob.ratly.jdbc.exception.SessionAlreadyClosedException;
import net.xdob.ratly.jdbc.util.SQLUtil2;
import net.xdob.ratly.proto.jdbc.ResultSetProto;
import net.xdob.ratly.protocol.Value;
import net.xdob.ratly.proto.jdbc.*;
import net.xdob.ratly.security.crypto.password.PasswordEncoder;
import net.xdob.ratly.server.exception.DbErrorException;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.storage.FileInfo;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.statemachine.impl.FileListStateMachineStorage;
import net.xdob.ratly.util.*;
import org.h2.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class InnerDb implements AutoCloseable {
	static final Logger LOG = LoggerFactory.getLogger(InnerDb.class);

//  public static final String SESSIONS_KEY = "sessions";
//	public static final String SESSIONS_JSON_EXT = "sessions.json";
  public static final String SQL_EXT = "sql";
  public static final String DB_EXT = "mv.db";
  public static final String INNER_USER = "remote";
  public static final String INNER_PASSWORD = "hhrhl2016";
	public static final String TRACE_DB_EXT = "trace.db";

	private final HikariConfig dsConfig = new HikariConfig();
  private volatile HikariDataSource dataSource = null;


  private final Path dbStore;
  private final DbInfo dbInfo;

  private DbsContext context;

  private final AtomicBoolean initialized = new AtomicBoolean(false);

  private final ClassCache classCache4DPM = new ClassCache();

  public int maxPoolSize = 64;

  public InnerDb(Path dbStore, DbInfo dbInfo, DbsContext context) {
    this.dbStore = dbStore;
    this.dbInfo = dbInfo;
    this.context = context;

  }

	public int getMaxPoolSize() {
		return maxPoolSize;
	}

	public InnerDb setMaxPoolSize(int maxPoolSize) {
		this.maxPoolSize = maxPoolSize;
		if(isInitialized()){
			closeDs();
			openDs();
		}
		return this;
	}

	public DbInfo getDbInfo() {
    return dbInfo;
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
            + ";AUTO_SERVER=TRUE;LOCK_TIMEOUT=6000";
            //+ ";TRACE_LEVEL_FILE=3;QUERY_TIMEOUT=600";     // 查询超时设置为 10 分钟（单位：秒）


        dsConfig.setPoolName(this.context.getPeerId()+"$"+getName());
        dsConfig.setJdbcUrl(url);
        dsConfig.setUsername(INNER_USER);
        dsConfig.setPassword(INNER_PASSWORD);

        // 2. 可选：优化配置
        dsConfig.setConnectionTimeout(30_000);    // 连接超时(ms)
        dsConfig.setIdleTimeout(60_000);         // 空闲超时(ms)
        dsConfig.setMaximumPoolSize(maxPoolSize);         // 最大连接数
        dsConfig.setMinimumIdle(5);              // 最小空闲连接
        dsConfig.addDataSourceProperty("cachePrepStmts", "true"); //
        dsConfig.setRegisterMbeans(true);
				deleteDbFile();
        openDs();
      } catch (Exception e) {
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

	private void deleteFile(File dbFile) {
		deleteFile(dbFile, 3);
	}

	private void deleteFile(File dbFile, int tryCount) {
		if(dbFile.exists()){
			if(!dbFile.delete()){
				if(tryCount>1){
					deleteFile(dbFile, tryCount-1);
				}else {
					LOG.warn("delete file failed {}", dbFile);
				}
			}else{
				LOG.info("delete file success {}", dbFile);
			}
		}
	}

	private void closeDs() {
    if(dataSource!=null){
      LOG.info("close db ds {} for {}", dataSource, getName());
      dataSource.close();
      dataSource = null;
			List<Session> sessions = context.getSessionMgr()
					.getAllSessions(getName());
			for (Session session : sessions) {
				session.inactive();
			}
		}
  }

	/**
	 * 获取插件内最早事务的开始索引
	 */
	public long getFirstTx(){
		return context.getSessionMgr().getFirstTx();
	}

	/**
	 * 获取插件已结束事务的索引
	 */
	public List<Long> getLastEndedTxIndexList(){
		return context.getSessionMgr().getLastEndedTxIndexList();
	}


	public void preSession(JdbcRequestProto request, JdbcResponseProto.Builder response) throws SQLException {
		OpenSessionProto openSession = request.getOpenSession();
		String user = openSession.getUser();
		String password = context.getRsaHelper().decrypt(openSession.getPassword());
		DbUser dbUser = getDbInfo().getUser(user).orElse(null);
		if (dbUser == null) {
			throw new AuthorizationFailedException();
		} else {
			PasswordEncoder passwordEncoder = context.getPasswordEncoder();
			if (!passwordEncoder.matches(password, dbUser.getPassword())) {
				throw new AuthorizationFailedException();
			} else {
				if (passwordEncoder.upgradeEncoding(dbUser.getPassword())) {
					dbUser.setPassword(passwordEncoder.encode(password));
					context.updateDbs();
				}
			}
		}
		int availableSessionCount = context.getSessionMgr().getAvailableSessionCount(getName());
		if(availableSessionCount <10){
			throw new TooManySessionException();
		}
		response.setValue(Value.toValueProto(availableSessionCount));
	}


	public void query(JdbcRequestProto request, JdbcResponseProto.Builder response) throws SQLException {

    String sessionId = request.getSessionId();
		response.setDb(request.getDb())
				.setSessionId(sessionId);
    Session session = context.getSessionMgr().getSession(sessionId).orElse(null);
			if(session==null){
				Printer4Proto.printJson(request, m->{
					LOG.info("session sessionId={} not found, request={} ", sessionId, m);
				});
				throw new SessionAlreadyClosedException(sessionId, "query");
			}else {
				session.heartBeat();
				query(request, session, response);
			}
  }


  private void query(JdbcRequestProto requestProto, Session session, JdbcResponseProto.Builder response) throws SQLException {
		if(requestProto.hasConnRequest()){
			ConnRequestProto connRequest = requestProto.getConnRequest();
			if(connRequest.getType()==ConnRequestType.databaseMeta){
				databaseMetaData(session, connRequest, response);
			}else if(connRequest.getType()==ConnRequestType.getAutoCommit){
				ConnectionResponse connectionProto = ConnectionResponse.newBuilder()
						.setAutoCommit(session.getConnection().getAutoCommit())
						.build();
				response.setConnection(connectionProto);
			}else if(connRequest.getType()==ConnRequestType.getTransactionIsolation){
				ConnectionResponse connectionProto = ConnectionResponse.newBuilder()
						.setTransactionIsolation(session.getConnection().getTransactionIsolation())
						.build();
				response.setConnection(connectionProto);
			}else if(connRequest.getType()==ConnRequestType.getWarnings){
				ConnectionResponse.Builder builder = ConnectionResponse.newBuilder();
				SQLWarning warnings = session.getConnection().getWarnings();
				if(warnings!=null) {
					builder.setSQLWarning(Proto2Util.toThrowable2Proto(warnings));
				}
				response.setConnection(builder.build());
			}else if(connRequest.getType()==ConnRequestType.clearWarnings){
				session.getConnection().clearWarnings();
			}else if(connRequest.getType()==ConnRequestType.nativeSQL){
				ConnectionResponse connectionProto = ConnectionResponse.newBuilder()
						.setSql(session.getConnection().nativeSQL(connRequest.getSql()))
						.build();
				response.setConnection(connectionProto);
			}
    }else if(requestProto.hasResultSet2Request()){
			ResultSet2RequestProto resultSetRequest = requestProto.getResultSet2Request();
			resultset2(session, resultSetRequest, response);
		}else{
      SqlRequestProto sqlRequest = requestProto.getSqlRequest();
      if(sqlRequest.getType().equals(SqlRequestType.parameterMeta)){
        parameterMeta(session, requestProto, response);
      }else if(sqlRequest.getType().equals(SqlRequestType.resultSetMetaData)){
        resultSetMetaData(session, requestProto, response);
      } if(sqlRequest.getType().equals(SqlRequestType.query)){
				try {
					String sql = sqlRequest.getSql();
					session.running(sql);
					SQLStatement statement = parseSql(sql);
					if(statement instanceof SQLShowSessionStatement){
						SerialResultSet resultSet = new SerialResultSet(Session.buildSessionResultSetMetaData());
						for (Session s : context.getSessionMgr().getAllSessions()) {
							resultSet.addRows(s.toSerialRow());
						}
						response.setResultSet(resultSet.toProto());
					}else {
						doQuery(session, sqlRequest, response, null,0);
					}
				}finally {
					session.sleep();
				}
      }
    }
  }

	private void resultset2(Session session, ResultSet2RequestProto requestProto,  JdbcResponseProto.Builder response) throws SQLException {
		String uid = requestProto.getUid();
		ResultSetMethod method = requestProto.getMethod();
		if(method==ResultSetMethod.close){
			RemoteResultSet2Proto.Builder builder = RemoteResultSet2Proto.newBuilder()
					.setUid(uid)
					.setValue(Value.toValueProto(null));
			response.setRemoteResultSet2(builder);
		}else if(method==ResultSetMethod.loadData) {
			SqlRequestProto sqlRequest = requestProto.getSqlRequest();
			doQuery(session, sqlRequest, response, uid, requestProto.getStart());
		}
	}

	private void handleResultSet2(ResultSet resultSet, RemoteResultSet2Proto.Builder builder, int start, int pageSize) throws SQLException {
		List<SerialRow> rows = getSerialRows4FetchSize(resultSet, start, pageSize);
		for (SerialRow row2 : rows) {
			builder.addRows(row2.toProto());
		}
		builder.setAllLoaded((resultSet.getRow()<1)|| resultSet.isAfterLast());
	}


	private List<SerialRow> getSerialRows4FetchSize(ResultSet resultSet, int start, int pageSize) throws SQLException {

		List<SerialRow> rows = new ArrayList<>();
		int rowNum = 0;
		while (resultSet.next()
		&& rows.size()<pageSize) {
			rowNum++;
			if(rowNum>start){
				SerialRow row = getRow(resultSet.getMetaData().getColumnCount(), resultSet);
				rows.add(row);
			}
		}
		return rows;
	}

	private SerialRow getRow(int columnCount, ResultSet rs) throws SQLException {
		if(rs.getRow()>=0) {
			SerialRow row = new SerialRow(columnCount);
			for (int col = 1; col <= columnCount; col++) {
				row.setValue(col - 1, rs.getObject(col));
			}
			return row;
		}else{
			return null;
		}
	}

  private void databaseMetaData(Session session, ConnRequestProto connRequest,  JdbcResponseProto.Builder response) throws SQLException {
    DatabaseMetaData metaData = session.getConnection().getMetaData();
    try {
      DatabaseMetaRequestProto databaseMetaRequest = connRequest.getDatabaseMetaRequest();
      String methodName  = databaseMetaRequest.getMethod();
      Class<?>[] paramTypes = new Class[databaseMetaRequest.getParametersTypesCount()];
      for (int i = 0; i < databaseMetaRequest.getParametersTypesList().size(); i++) {
        String parametersType = databaseMetaRequest.getParametersTypes(i);
        paramTypes[i] = convertType(parametersType);
      }

      Method method = classCache4DPM.getMethod(metaData.getClass(), methodName, paramTypes);;
      Object[] args = new Object[databaseMetaRequest.getArgsCount()];
      for (int i = 0; i < databaseMetaRequest.getArgsList().size(); i++) {
        args[i] = Value.toJavaObject(databaseMetaRequest.getArgs(i));
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

	//start从0开始
  private void doQuery(Session session, SqlRequestProto sqlRequest,  JdbcResponseProto.Builder response, String uid, int start) throws SQLException {

		int pageSize = 200;
		String sql = sqlRequest.getSql();

		if(StmtType.prepared.equals(sqlRequest.getStmtType())
        ||StmtType.callable.equals(sqlRequest.getStmtType())){
      try(CallableStatement stmt = session.getConnection().prepareCall(sql)) {

				stmt.setQueryTimeout(60);
        List<ParameterProto> paramList = sqlRequest.getParams().getParamList();
        if(!paramList.isEmpty()){
          for (ParameterProto parameterProto : paramList) {
            Parameter parameter = Parameter.from(parameterProto);
            Object value = parameter.getValue();
            stmt.setObject(parameter.getIndex(), value);
          }
        }
        ResultSet rs = stmt.executeQuery();
				rs.setFetchSize(sqlRequest.getFetchSize());
				handleResultSet(response, rs, uid, pageSize, start);
			}catch (SQLException e){
				Printer4Proto.printText(sqlRequest, s->LOG.info("sqlRequest:{} ", s));
				LOG.warn("sql:{} executeQuery error.", sql, e);
				throw e;
			}
    }else{
      try(Statement stmt = session.getConnection().createStatement()){
				stmt.setQueryTimeout(60);
        ResultSet rs = stmt.executeQuery(sql);

				rs.setFetchSize(sqlRequest.getFetchSize());

				handleResultSet(response, rs, uid, pageSize, start);
      }catch (SQLException e){
				Printer4Proto.printText(sqlRequest, s->LOG.info("sqlRequest:{} ", s));
				LOG.warn("sql:{} executeQuery error.", sql, e);
				throw e;
			}
    }

  }

	private void handleResultSet(JdbcResponseProto.Builder response, ResultSet rs, String uid, int pageSize, int start) throws SQLException {
		if(uid==null){
			uid = UUID.randomUUID().toString();
		}
		SerialResultSetMetaData metaData = new SerialResultSetMetaData(rs.getMetaData());
		RemoteResultSet2Proto.Builder builder = RemoteResultSet2Proto.newBuilder()
				.addAllColumns(metaData.toProto())
				.setUid(uid)
				.setStart(start);
		handleResultSet2(rs, builder, start, pageSize);
		response.setRemoteResultSet2(builder);
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
				String sessionId = String.valueOf(termIndex.getIndex());
        String user = openSession.getUser();
        String password = context.getRsaHelper().decrypt(openSession.getPassword());
        DbUser dbUser = getDbInfo().getUser(user).orElse(null);
        if (dbUser == null) {
          throw new AuthorizationFailedException();
        } else {
          PasswordEncoder passwordEncoder = context.getPasswordEncoder();
          if (!passwordEncoder.matches(password, dbUser.getPassword())) {
            throw new AuthorizationFailedException();
          } else {
            if (passwordEncoder.upgradeEncoding(dbUser.getPassword())) {
              dbUser.setPassword(passwordEncoder.encode(password));
              context.updateDbs();
            }
          }
        }


				Session session = context.getSessionMgr().newSession(request.getDb(), user, sessionId, MemoizedCheckedSupplier.valueOf(this::getConnection));
        response.setSessionId(session.getSessionId());
      }else if(connRequest.getType()==ConnRequestType.closeSession){
        String sessionId = request.getSessionId();
				context.getSessionMgr().closeSession(sessionId, termIndex.getIndex());
      }else{
        String sessionId = request.getSessionId();
        Session session = context.getSessionMgr().getSession(sessionId)
            .orElseThrow(()->new SessionAlreadyClosedException(sessionId, "conn"));
				session.heartBeat();
				session.updateAppliedIndexToMax(termIndex.getIndex());
        applyLog4Conn(termIndex, session, connRequest, response);
      }
    }else if(request.hasSqlRequest()) {
			SqlRequestProto sqlRequest = request.getSqlRequest();
			String sessionId = request.getSessionId();
			Session session = context.getSessionMgr().getSession(sessionId)
					.orElseThrow(() -> new SessionAlreadyClosedException(sessionId, "sql"));
			session.heartBeat();
			session.updateAppliedIndexToMax(termIndex.getIndex());
			try {
				if (session.isTransaction()) {
					session.updateTx(termIndex.getIndex());
					executeUpdate(termIndex, sqlRequest, session, response);
				} else {
					applyLog4Sql(termIndex, session, sqlRequest, response);
				}
			} finally {
				session.sleep();
			}
    }

  }

  Connection getConnection() throws SQLException {
    LOG.info("get connection from {}", dataSource);
    return dataSource.getConnection();
  }


  private void applyLog4Conn(TermIndex termIndex, Session session, ConnRequestProto connRequest, JdbcResponseProto.Builder response) throws SQLException {

		if(ConnRequestType.setAutoCommit.equals(connRequest.getType())){
			session.setAutoCommit(connRequest.getAutoCommit());
		}else if(ConnRequestType.setTransactionIsolation.equals(connRequest.getType())){
			session.setTransactionIsolation(connRequest.getTransactionIsolation());
		}else if(ConnRequestType.commit.equals(connRequest.getType())){
			session.commit(termIndex.getIndex());
    }else if(ConnRequestType.rollback.equals(connRequest.getType())){
			session.rollback(termIndex.getIndex());
    }else if(ConnRequestType.savepoint.equals(connRequest.getType())){
      Savepoint s = JdbcSavepoint.from(connRequest.getSavepoint());
      Savepoint savepoint = session.setSavepoint(s.getSavepointName());
      response.setSavepoint(JdbcSavepoint.of(savepoint).toProto());
    }else if(ConnRequestType.releaseSavepoint.equals(connRequest.getType())){
      Savepoint savepoint = JdbcSavepoint.from(connRequest.getSavepoint());
      session.releaseSavepoint(savepoint);
    }else if(ConnRequestType.rollbackSavepoint.equals(connRequest.getType())){
      Savepoint savepoint = JdbcSavepoint.from(connRequest.getSavepoint());
			session.rollback(savepoint);
    }
  }

  private void applyLog4Sql(TermIndex termIndex, Session session, SqlRequestProto sqlRequest, JdbcResponseProto.Builder response) throws SQLException {

    try {
      executeUpdate(termIndex, sqlRequest, session, response);
    } catch (SQLException e) {
      response.setEx(Proto2Util.toThrowable2Proto(e));
    }
  }

  private void executeUpdate(TermIndex termIndex, SqlRequestProto request, Session session,
                             JdbcResponseProto.Builder response) throws SQLException {

		if (StmtType.prepared.equals(request.getStmtType())
				|| StmtType.callable.equals(request.getStmtType())) {
			String sql = request.getSql();
			//LOG.info("execute sql:{}", sql);
			SQLStatement statement = parseSql(sql);
			if(isRollback(statement)){
				session.rollback(termIndex.getIndex());
			}else if(isCommit( statement)){
				session.commit(termIndex.getIndex());
			}else{
				update4Prepared(termIndex, session, request, response);
				if(isDdl(statement)){
					session.commit(termIndex.getIndex());
				}
			}
		} else {
			update4Statement(termIndex, session, request, response);
		}
  }

  void update4Prepared(TermIndex termIndex, Session session,
                       SqlRequestProto sqlRequest,
                       JdbcResponseProto.Builder response) throws SQLException {
		String sql = sqlRequest.getSql();

		try (PreparedStatement ps = session.getConnection().prepareStatement(sql)) {
      ps.setQueryTimeout(60);
      if (!sqlRequest.getBatchParamsList().isEmpty()) {
				session.running("execute Batch:" + sqlRequest.getSql());
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
				session.running(sqlRequest.getSql());
        setParams(ps, Parameters.from(sqlRequest.getParams()));
        long updateCount = ps.executeLargeUpdate();
        response.setUpdateCount(updateCount);
      }
    }

	}

	private boolean isDdl(SQLStatement stmt) {
		return SQLUtil2.getSqlType( stmt).isDDL();
	}

	private boolean isCommit(SQLStatement stmt) {
		return SQLUtil2.isCommit( stmt);
	}

	private boolean isRollback(SQLStatement stmt) {
		return SQLUtil2.isRollback( stmt);
	}

	private void setParams(PreparedStatement ps, Parameters paramList) throws SQLException {
    if(!paramList.isEmpty()) {
      for (Parameter param : paramList.getParameters()) {
        ps.setObject(param.getIndex(), param.getValue());
      }
    }
  }

	private SQLStatement parseSql(String sql) throws SQLException {
		return SQLUtil2.parse(sql);
	}


  void update4Statement(TermIndex termIndex,
												Session session,
                        SqlRequestProto sqlRequest,
                        JdbcResponseProto.Builder response) throws SQLException {

		try (Statement s = session.getConnection().createStatement()) {
      s.setQueryTimeout(60);
      if (!sqlRequest.getBatchSqlList().isEmpty()) {
				session.running("execute Batch sql");
        for (String sql : sqlRequest.getBatchSqlList()) {
					SQLStatement statement = parseSql(sql);
					if (isRollback(statement)
							|| isCommit(statement)
							|| isDdl(statement)) {
						throw new SQLException("rollback or commit or ddl not allowed in batch mode");
					}
					//LOG.info("execute sql:{}", sql);
          s.addBatch(sql);
        }
        long[] updateCounts = s.executeLargeBatch();
        UpdateCounts.Builder builder = UpdateCounts.newBuilder();
        for (long updateCount : updateCounts) {
          builder.addUpdateCount(updateCount);
        }
        response.setUpdateCounts(builder);
      } else {
				String sql = sqlRequest.getSql();
				//LOG.info("execute sql:{}", sql);
				session.running(sqlRequest.getSql());
				SQLStatement statement = parseSql(sql);
				if(isRollback(statement)){
					session.rollback(termIndex.getIndex());
				}else if(isCommit( statement)){
					session.commit(termIndex.getIndex());
				}else {
					long updateCount = s.executeLargeUpdate(sqlRequest.getSql());
					response.setUpdateCount(updateCount);
					if(statement instanceof SQLBeginStatement){
						session.beginTx();
					}
					if(isDdl(statement)){
						session.commit(termIndex.getIndex());
					}
				}
      }
    }
  }



  public List<FileInfo> takeSnapshot(FileListStateMachineStorage storage, TermIndex last) throws IOException {
		try {
			context.getSessionMgr().setDisabled(true);
			return takeSnapshot2(storage, last);
		}finally {
			context.getSessionMgr().setDisabled(false);
		}
	}

	public void finishSnapshot(FileListStateMachineStorage storage, TermIndex last, List<FileInfo> infos) throws IOException
	{
		String dbModule = getName() + "." + DB_EXT;
		FileInfo dbFileInfo = infos.stream()
				.filter(fileInfo -> dbModule.equals(fileInfo.getModule()))
				.findFirst().orElse(null);
		if(dbFileInfo!=null){
			Stopwatch stopwatch = Stopwatch.createStarted();
			//生成sql快照
			String sqlModule = getName() + "." + SQL_EXT;
			File sqlFile = storage.getSnapshotFile(sqlModule, last.getTerm(), last.getIndex());
			FileInfo sqlFileInfo = new FileInfo(sqlFile.toPath(), null, sqlModule);
			infos.add(sqlFileInfo);

			File dbPath = storage.getSnapshotFile(getName(), last.getTerm(), last.getIndex());

			// 基于存储目录初始化
			String url = "jdbc:h2:file:" + dbPath.toString()
					+ ";QUERY_TIMEOUT=600";
			try (Connection connection = DriverManager.getConnection(url, INNER_USER, INNER_PASSWORD);
					 Statement stmt = connection.createStatement()) {
				stmt.setQueryTimeout(600);
				stmt.execute("SCRIPT DROP TO '" + sqlFile.toString() + "'");
				//关闭前让数据落盘  CHECKPOINT SYNC
				stmt.executeUpdate("checkpoint sync");
			}catch (SQLException e){
				LOG.warn("takeSqlSnapshot error", e);
				throw DbErrorException.error(dbFileInfo.toString(), e);
			}
			Digest digest = MD5FileUtil.computeAndSaveDigestForFile(sqlFile);
			sqlFileInfo.setFileDigest(digest);
			LOG.info("takeSqlSnapshot to file {}, use time:{}", sqlFile.toString(), stopwatch);
			stopwatch.reset().start();
//			infos.removeIf(fileInfo -> fileInfo.equals(dbFileInfo));
//			dbFileInfo.getPath().toFile().delete();
			computeAndSaveDigestForFile(dbFileInfo);
			LOG.info("computeAndSaveDigestForFile for file {}, use time:{}", dbFileInfo.toString(), stopwatch);
		}


	}

	private List<FileInfo> takeSnapshot2(FileListStateMachineStorage storage, TermIndex last) throws IOException {
		List<FileInfo> infos = new ArrayList<>();
		Stopwatch stopwatch = Stopwatch.createStarted();
		try(Connection connection = getConnection();
				Statement stmt = connection.createStatement()){
			stmt.setQueryTimeout(60);
			//快照前让数据落盘  CHECKPOINT SYNC
			stmt.executeUpdate("checkpoint sync");
		}catch (SQLException e){
			throw new IOException(e);
		}

		String module = getName() + "." + DB_EXT;
		File dbFile = storage.getSnapshotFile(module, last.getTerm(), last.getIndex());
		Path sourceDbFile = dbStore.resolve(module);

		if(!sourceDbFile.toFile().exists()){
			throw DbErrorException.notExists(sourceDbFile.toString());
		}
		if(sourceDbFile.toFile().length()<128){
			throw DbErrorException.error(sourceDbFile.toString());
		}
		Files.copy(sourceDbFile, dbFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

		FileInfo dbFileInfo = new FileInfo(dbFile.toPath(), null, module);
		infos.add(dbFileInfo);

		LOG.info("{} Taking a DB snapshot, use time:{}", getName(), stopwatch);
		return infos;
	}

	private void computeAndSaveDigestForFile(FileInfo dbFileInfo) {
		Digest digest2 = MD5FileUtil.computeAndSaveDigestForFile(dbFileInfo.getPath().toFile());
		dbFileInfo.setFileDigest(digest2);
	}


	private FileInfo getFileInfo(File file, String module) {
    final Digest digest = MD5FileUtil.computeAndSaveDigestForFile(file);
		return new FileInfo(file.toPath(), digest, module);
  }


  public void restoreFromSnapshot(SnapshotInfo snapshot) throws IOException {
		try {
			context.getSessionMgr().setDisabled(true);
			restoreFromSnapshot2(snapshot);
		} finally {
			context.getSessionMgr().setDisabled(false);
		}
	}

	private void restoreFromSnapshot2(SnapshotInfo snapshot) throws IOException {
		if(snapshot==null){
			return;
		}
		deleteDbFile();
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
					LOG.warn("DB sql snapshot digest mismatch, expected {}, actual {}", sqlfileInfo.getFileDigest(), digest);
				}
			}
		}
		LOG.info("{} restore DB snapshot use time: {}", getName(), stopwatch);
	}

	private void deleteDbFile() {
		String dbPath = dbStore.resolve(getName()).toString();
		File dbFile = new File(dbPath + "." + DB_EXT);
		deleteFile(dbFile);
		File traceFile = new File(dbPath + "." + TRACE_DB_EXT);
		deleteFile(traceFile);
	}


	public void close() {
		try {
			closeDs();
			context = null;
		} catch (Exception e) {
			LOG.warn("{} close error", getName(), e);
		}

  }


}
