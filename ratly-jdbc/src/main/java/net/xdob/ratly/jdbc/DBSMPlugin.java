package net.xdob.ratly.jdbc;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.io.Digest;
import net.xdob.ratly.jdbc.exception.NoDatabaseException;
import net.xdob.ratly.jdbc.sql.JdbcConnection;
import net.xdob.ratly.json.Jsons;
import net.xdob.ratly.proto.jdbc.*;
import net.xdob.ratly.proto.sm.*;
import net.xdob.ratly.protocol.*;
import net.xdob.ratly.security.crypto.password.PasswordEncoder;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.storage.FileInfo;
import net.xdob.ratly.server.storage.RaftStorage;
import net.xdob.ratly.security.RsaHelper;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.statemachine.impl.FileListStateMachineStorage;
import net.xdob.ratly.statemachine.impl.SMPlugin;
import net.xdob.ratly.statemachine.impl.SMPluginContext;
import net.xdob.ratly.util.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public class DBSMPlugin implements SMPlugin, DbsContext {


  public static final String DB = "db";
  public static final String DBS = "dbs";
  public static final String DBS_JSON = DBS + ".json";
	public static final String SHOW_SESSION = "show session";
	public static final String KILL_SESSION = "kill session";
	public static final String SESSIONS = "sessions";
	public static final String SESSIONS_MODULE = SESSIONS + ".json";

	private Path dbStore;
  private Path dbCache;
  private SMPluginContext context;

  private final Map<String,InnerDb> dbMap = Maps.newConcurrentMap();

  private final Map<String, DbDef> dbDefs = Maps.newConcurrentMap();;
  private final RsaHelper rsaHelper = new RsaHelper();

  private boolean dynamicCreate = false;
	private DefaultSessionMgr sessionMgr;


	public DBSMPlugin() {
  }

  @Override
  public String getId() {
    return DB;
  }

  public boolean isDynamicCreate() {
    return dynamicCreate;
  }

  public void setDynamicCreate(boolean dynamicCreate) {
    this.dynamicCreate = dynamicCreate;
  }

  @Override
  public void setSMPluginContext(SMPluginContext context) {
    this.context = context;
  }

	@Override
	public String getLeaderId() {
		return Optional.ofNullable(context.getLeaderId())
				.map(RaftPeerId::getId)
				.orElse("");
	}

	@Override
	public String getPeerId() {
		return Optional.ofNullable(context.getPeerId())
				.map(RaftPeerId::getId)
				.orElse("");
	}

	@Override
	public ScheduledExecutorService getScheduler() {
		return context.getScheduler();
	}

	@Override
	public SnapshotInfo getLatestSnapshot() {
		return context.getLatestSnapshot();
	}

	@Override
	public PasswordEncoder getPasswordEncoder() {
		return context.getPasswordEncoder();
	}

	@Override
	public RsaHelper getRsaHelper() {
		return rsaHelper;
	}


	@Override
	public void updateDbs() {
		saveDbs();
	}


	@Override
	public void stopServerState() {
		context.stopServerState();
	}

	@Override
	public TermIndex getLastAppliedTermIndex() {
		return context.getLastAppliedTermIndex();
	}

	@Override
	public RaftClient getRaftClient() {
		return context.getRaftClient();
	}

	@Override
	public void closeSession(String db, String sessionId) {
		if(!context.getPeerId().equals(context.getLeaderId())){
			return;
		}
		try {
			ConnRequestProto.Builder connReqBuilder = ConnRequestProto.newBuilder()
					.setType(ConnRequestType.closeSession);
			JdbcRequestProto requestProto = JdbcRequestProto.newBuilder()
					.setDb(db)
					.setSessionId(sessionId)
					.setConnRequest(connReqBuilder.build())
					.build();
			WrapRequestProto wrap = WrapRequestProto.newBuilder()
					.setType(JdbcConnection.DB)
					.setJdbcRequest(requestProto)
					.build();
			RaftClientReply reply = context.getRaftClient().io()
					.send(Message.valueOf(wrap));
			WrapReplyProto replyProto = WrapReplyProto.parseFrom(reply.getMessage().getContent());
			JdbcResponseProto response = replyProto.getJdbcResponse();
			if(response.hasEx()){
				throw Proto2Util.toSQLException(response.getEx());
			}
		} catch (Exception e) {
			LOG.warn("close session error", e);
		}
	}

	@Override
	public SessionMgr getSessionMgr() {
		return sessionMgr;
	}

	@Override
	public int getMaxConnSize(String db) {
		return Optional.ofNullable(dbMap.get(db))
				.map(InnerDb::getMaxPoolSize).orElse(64);
	}

	@Override
  public void initialize(RaftServer server, RaftGroupId groupId, RaftPeerId peerId, RaftStorage raftStorage) throws IOException {

		sessionMgr = new DefaultSessionMgr(this);
		/**
     * 初始化数据库
     */
    this.dbStore = Paths.get(raftStorage.getStorageDir().getRoot().getPath(), "db");
    if(!dbStore.toFile().exists()){
      dbStore.toFile().mkdirs();
    }
    LOG.info("build db cache dir groupId={}, peerId={}", groupId, peerId);
    this.dbCache = Paths.get(raftStorage.getDirCache().getPath(),  groupId.getId(), "db", peerId.getId());
    if(!dbCache.toFile().exists()){
      dbCache.toFile().mkdirs();
    }

    File dbsFile = this.dbStore.resolve(DBS_JSON).toFile();
    loadDbs(dbsFile);
    for (DbDef dbDef : dbDefs.values()) {
      addDbIfAbsent(dbDef);
    }
    saveDbs();
  }

  private void addDbIfAbsent(DbDef dbDef) {
    dbMap.computeIfAbsent(dbDef.getDb(), n -> {
      DbInfo dbInfo = new DbInfo().setName(dbDef.getDb());
      String encode = context.getPasswordEncoder().encode(dbDef.getPassword());
      dbInfo.addUser(dbDef.getUser(), encode);
      InnerDb innerDb = new InnerDb(dbCache, dbInfo, this);
      innerDb.initialize();
      return innerDb;
    });
  }

  private void loadDbs(File dbsFile) throws IOException {
    if(dbsFile.exists()&& dbsFile.length()>0){
      Map<String,DbState> dbs = new HashMap<>();
      try(BufferedReader br = new BufferedReader(new InputStreamReader(
          FileUtils.newInputStream(dbsFile), StandardCharsets.UTF_8))){
        N3Map map = Jsons.i.fromJson(br, N3Map.class);
        List<DbState> dbStates = map.getValues(DbState.class, DBS);
        dbStates.forEach(dbState -> dbs.put(dbState.getName(), dbState));
      }
      for (DbState dbState : dbs.values()) {
        dbMap.computeIfAbsent(dbState.getName(), n -> {
          InnerDb innerDb2 = new InnerDb(dbCache, dbState.toDbInfo(), this);
          innerDb2.initialize();
          return innerDb2;
        });
      }
			for (String name : dbMap.keySet()) {
				if(!dbs.containsKey( name)){
					InnerDb removed = dbMap.remove(name);
					if(removed!=null){
						removed.close();
					}
				}
			}
    }
  }

  @Override
  public void reinitialize() throws IOException {
  }

  private void saveDbs(){
    File dbsFile = this.dbStore.resolve(DBS_JSON).toFile();
    saveDbsToFile(dbsFile);
  }

  private void saveDbsToFile(File dbsFile) {

    N3Map n3Map = new N3Map();

    List<DbInfo> dbStateList = dbMap.values().stream()
        .map(InnerDb::getDbInfo)
        .collect(Collectors.toList());
    n3Map.put(DBS, dbStateList);
		saveN3Map(dbsFile, n3Map);
	}

	@Override
	public void admin(WrapRequestProto request, WrapReplyProto.Builder reply) {
		if(request.hasJdbcRequest()) {
			JdbcResponseProto.Builder response = JdbcResponseProto.newBuilder();
			JdbcRequestProto jdbcRequest = request.getJdbcRequest();
			if(jdbcRequest.hasHeartbeat()) {
				try {
					String sessionId = jdbcRequest.getSessionId();
					response.setDb(jdbcRequest.getDb())
							.setSessionId(sessionId);
					this.heart(sessionId);
				} catch (SQLException e) {
					LOG.warn("", e);
					response.setEx(Proto2Util.toThrowable2Proto(e));
				}
			}else if(jdbcRequest.hasAdmin()){
				AdminProto admin = jdbcRequest.getAdmin();
				try {
					if (admin.getCmd().equalsIgnoreCase(SHOW_SESSION)) {
						List<Map<String, Object>> list = new ArrayList<>();
						this.showSessions(list);
						response.setValue(Value.toValueProto( list));
					} else if (admin.getCmd().equalsIgnoreCase(KILL_SESSION)) {
						String sessionId = admin.getArg0();
						boolean killed = this.killSession(sessionId);
						response.setUpdateCount(killed?1:0);
					}
				} catch (SQLException e) {
					LOG.warn("", e);
					response.setEx(Proto2Util.toThrowable2Proto(e));
				}
			} else if (jdbcRequest.hasPreSession()) {
				String db = jdbcRequest.getDb();
				InnerDb innerDb = dbMap.get(db);
				try {
					if(innerDb==null&&dynamicCreate){
						OpenSessionProto openSession = jdbcRequest.getOpenSession();
						String user = openSession.getUser();
						String password = rsaHelper.decrypt(openSession.getPassword());
						this.addDbIfAbsent(db, user, password);
						addDbIfAbsent(dbDefs.get(db));
						innerDb = dbMap.get(db);
					}
					if (innerDb != null) {
						innerDb.preSession(jdbcRequest, response);
					} else {
						throw new NoDatabaseException(db);
					}
				} catch (SQLException e) {
					LOG.warn("", e);
					response.setEx(Proto2Util.toThrowable2Proto(e));
				}
			}
			reply.setJdbcResponse(response);
		}
	}

	public void heart(String sessionId) throws SQLException {

		sessionMgr.getSession(sessionId)
				.ifPresent(Session::heartBeat);
	}

	public boolean killSession(String sessionId) throws SQLException {
		Session session = sessionMgr.getSession(sessionId).orElse(null);
		if(session!=null) {
			this.closeSession(session.getDb(), sessionId);
			return true;
		}
		return false;
	}

	public void showSessions(List<Map<String, Object>> list) throws SQLException {
		for (Session session : sessionMgr.getAllSessions()) {
			list.add(session.toMap());
		}
	}

	@Override
  public void query(WrapRequestProto request, WrapReplyProto.Builder reply)  {
    
		if(request.hasJdbcRequest()) {
			JdbcResponseProto.Builder response = JdbcResponseProto.newBuilder();
			JdbcRequestProto jdbcRequest = request.getJdbcRequest();
			String db = jdbcRequest.getDb();
			InnerDb innerDb = dbMap.get(db);
			try {
				if (innerDb != null) {
					innerDb.query(jdbcRequest, response);
				} else {
					throw new NoDatabaseException(db);
				}
			} catch (SQLException e) {
				LOG.warn("", e);
				response.setEx(Proto2Util.toThrowable2Proto(e));
			}
			reply.setJdbcResponse(response);
		}
	}

  @Override
  public void applyTransaction(TermIndex termIndex, WrapRequestProto request, WrapReplyProto.Builder reply)  {

    JdbcResponseProto.Builder response = JdbcResponseProto.newBuilder();
    JdbcRequestProto jdbcRequest = request.getJdbcRequest();
    String db = jdbcRequest.getDb();
    InnerDb innerDb = dbMap.get(db);
    if(innerDb==null&&dynamicCreate
        &&jdbcRequest.hasConnRequest()
        &&jdbcRequest.getConnRequest().getType()==ConnRequestType.openSession){
      OpenSessionProto openSession = jdbcRequest.getOpenSession();
      String user = openSession.getUser();
      String password = rsaHelper.decrypt(openSession.getPassword());
      this.addDbIfAbsent(db, user, password);
      addDbIfAbsent(dbDefs.get(db));
      saveDbs();
      innerDb = dbMap.get(db);
    }
    try {
      if(innerDb!=null){
        innerDb.applyTransaction(termIndex, jdbcRequest, response);
      } else{
        throw new NoDatabaseException(db);
      }
    } catch (SQLException e) {
      LOG.warn("", e);
      response.setEx(Proto2Util.toThrowable2Proto(e));
    }
    reply.setJdbcResponse(response);
  }

  public DBSMPlugin addDbIfAbsent(String db, String user, String password){
    dbDefs.computeIfAbsent(db, name->new DbDef(name, user, password));
    return this;
  }


  @Override
  public List<FileInfo> takeSnapshot(FileListStateMachineStorage storage, TermIndex last) throws IOException {
    Stopwatch stopwatch = Stopwatch.createStarted();
    ArrayList<FileInfo> fileInfos = Lists.newArrayList();

    final File snapshotFile =  storage.getSnapshotFile(DBS_JSON, last.getTerm(), last.getIndex());
    saveDbsToFile(snapshotFile);
    fileInfos.add(getFileInfo(snapshotFile, DBS_JSON));
    for (InnerDb innerDb : dbMap.values()) {
      List<FileInfo> infos = innerDb.takeSnapshot(storage, last);
      if(!infos.isEmpty()){
        fileInfos.addAll(infos);
      }
    }
		File sessionFile = storage.getSnapshotFile(SESSIONS_MODULE, last.getTerm(), last.getIndex());
		List<SessionData> sessionDataList = sessionMgr.getAllSessions().stream()
				.map(Session::toSessionData)
				.filter(Objects::nonNull)
				.collect(Collectors.toList());
		N3Map n3Map = new N3Map();
		n3Map.put(SESSIONS, sessionDataList);
		saveN3Map(sessionFile, n3Map);
		fileInfos.add(getFileInfo(sessionFile, SESSIONS_MODULE));
    LOG.info("Taking a DBS snapshot use time: {}", stopwatch);
    return fileInfos;
  }

	private static void saveN3Map(File file, N3Map n3Map) {
		try(BufferedWriter out = new BufferedWriter(
				new OutputStreamWriter(new AtomicFileOutputStream(file), StandardCharsets.UTF_8))){
			String json = Jsons.i.toJson(n3Map);
			out.write(json);
		}catch (IOException e) {
			LOG.warn("save file fail, file={}", file, e);
		}
	}

	private FileInfo getFileInfo(File file, String module) {
		final Digest digest = MD5FileUtil.computeAndSaveDigestForFile(file);
		return new FileInfo(file.toPath(), digest, module);
	}

	@Override
	public void finishSnapshot(FileListStateMachineStorage storage, TermIndex last, List<FileInfo> files) throws IOException {
		for (InnerDb innerDb : dbMap.values()) {
			innerDb.finishSnapshot(storage, last, files);
		}
	}

	@Override
  public void restoreFromSnapshot(SnapshotInfo snapshot) throws IOException {
    if(snapshot==null){
      return;
    }
    List<FileInfo> dbsFiles = snapshot.getFiles(DBS_JSON);
    if(!dbsFiles.isEmpty()) {
      FileInfo fileInfo = dbsFiles.get(0);
      final File snapshotFile = fileInfo.getPath().toFile();
      final Digest digest = MD5FileUtil.computeDigestForFile(snapshotFile);
      if (digest.equals(fileInfo.getFileDigest())) {
        if(snapshotFile.exists()){
          loadDbs(snapshotFile);
        }
      }
    }
		Stopwatch stopwatch = Stopwatch.createStarted();
		for (InnerDb innerDb : dbMap.values()) {
			innerDb.restoreFromSnapshot(snapshot);
		}

		FileInfo sessionfileInfo = snapshot.getFiles(SESSIONS_MODULE).stream().findFirst().orElse(null);
		if(sessionfileInfo!=null) {
			final File sessionFile = sessionfileInfo.getPath().toFile();
			final Digest digest = MD5FileUtil.computeDigestForFile(sessionFile);
			if (digest.equals(sessionfileInfo.getFileDigest())) {
				byte[] bytes = Files.readAllBytes(sessionFile.toPath());
				N3Map n3Map = Jsons.i.fromJson(new String(bytes, StandardCharsets.UTF_8), N3Map.class);
				List<SessionData> sessions = n3Map.getValues(SessionData.class, SESSIONS);
				for (SessionData sessionData : sessions) {
					try {
						String db = sessionData.getDb();
						Session session = sessionMgr.getSession(sessionData.getSessionId()).orElse(null);
						if(session==null) {

							session = sessionMgr.newSession(db, sessionData.getUser(), sessionData.getSessionId(),
									MemoizedCheckedSupplier.valueOf(()->{
										InnerDb innerDb = dbMap.get(db);
										if(innerDb==null){
											throw new NoDatabaseException(db);
										}
										return innerDb.getConnection();
									}));
							session.setSessionData(sessionData);
						}
						session.heartBeat();
						LOG.info("{} restore session {}", db, sessionData.getSessionId());
					} catch (SQLException e) {
						LOG.warn("node {} newSession error.", context.getPeerId(), e);
					}
				}

			}
		}
		LOG.info("restore DBS snapshot use time: {}", stopwatch);

  }


  @Override
  public void close() throws IOException {
    for (InnerDb innerDb : dbMap.values()) {
      innerDb.close();
    }
    dbMap.clear();
  }

	@Override
	public List<Long> getLastEndedTxIndexList() {
		return dbMap.values().stream()
				.flatMap(e -> e.getLastEndedTxIndexList().stream())
				.filter(e->e>0)
				.collect(Collectors.toList());
	}

	@Override
	public long getFirstTx() {
		return dbMap.values().stream()
				.map(InnerDb::getFirstTx)
				.filter(e-> e > RaftLog.INVALID_LOG_INDEX)
				.min(Long::compareTo)
				.orElse(RaftLog.INVALID_LOG_INDEX);
	}
}
