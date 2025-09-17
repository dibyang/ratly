package net.xdob.jdbc;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.io.Digest;
import net.xdob.jdbc.exception.NoDatabaseException;
import net.xdob.ratly.json.Jsons;
import net.xdob.ratly.proto.jdbc.*;
import net.xdob.ratly.proto.sm.*;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.protocol.Value;
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
import net.xdob.ratly.util.Timestamp;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public class DBSMPlugin implements SMPlugin {


  public static final String DB = "db";
  public static final String DBS = "dbs";
  public static final String DBS_JSON = DBS + ".json";
	public static final String SHOW_SESSION = "show session";
	public static final String KILL_SESSION = "kill session";
	public static final String SHOW_SNAPSHOT_INDEX = "show snapshot index";


	private Path dbStore;
  private Path dbCache;
  private SMPluginContext context;

  private final Map<String,InnerDb> dbMap = Maps.newConcurrentMap();

  private final Map<String, DbDef> dbDefs = Maps.newConcurrentMap();;
  private final RsaHelper rsaHelper = new RsaHelper();
	private PauseLastTime pauseLastTime;

  private DbsContext dbsContext;
  private boolean dynamicCreate = false;


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
		this.pauseLastTime = new GCLastTime();
    this.dbsContext = new DbsContext() {
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
			public Timestamp getLastPauseTime() {
				return pauseLastTime.getLastPauseTime();
			}
		};
  }

  @Override
  public void initialize(RaftServer server, RaftGroupId groupId, RaftPeerId peerId, RaftStorage raftStorage) throws IOException {
		this.pauseLastTime.start();

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
      InnerDb innerDb = new InnerDb(dbCache, dbInfo, dbsContext);
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
        InnerDb innerDb = dbMap.computeIfAbsent(dbState.getName(), n -> {
          InnerDb innerDb2 = new InnerDb(dbCache, dbState.toDbInfo(), dbsContext);
          innerDb2.initialize();
          return innerDb2;
        });
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
    try(BufferedWriter out = new BufferedWriter(
        new OutputStreamWriter(new AtomicFileOutputStream(dbsFile), StandardCharsets.UTF_8))){
      String json = Jsons.i.toJson(n3Map);
      out.write(json);
    }catch (IOException e) {
      LOG.warn("", e);
    }
  }

	@Override
	public void admin(WrapRequestProto request, WrapReplyProto.Builder reply) {
		if(request.hasJdbcRequest()) {
			JdbcResponseProto.Builder response = JdbcResponseProto.newBuilder();
			JdbcRequestProto jdbcRequest = request.getJdbcRequest();
			if(jdbcRequest.hasHeartbeat()) {
				String db = jdbcRequest.getDb();
				InnerDb innerDb = dbMap.get(db);
				try {
					if (innerDb != null) {
						String sessionId = jdbcRequest.getSessionId();
						response.setDb(jdbcRequest.getDb())
								.setSessionId(sessionId);
						innerDb.heart(sessionId);
					} else {
						throw new NoDatabaseException(db);
					}
				} catch (SQLException e) {
					LOG.warn("", e);
					response.setEx(Proto2Util.toThrowable2Proto(e));
				}
			}else if(jdbcRequest.hasAdmin()){
				AdminProto admin = jdbcRequest.getAdmin();
				try {
					if (admin.getCmd().equalsIgnoreCase(SHOW_SESSION)) {
						List<Map<String, Object>> list = new ArrayList<>();
						for (InnerDb db : dbMap.values()) {
							db.showSessions(list);
						}
						response.setValue(Value.toValueProto( list));
					} else if (admin.getCmd().equalsIgnoreCase(KILL_SESSION)) {
						boolean killed = false;
						String sessionId = admin.getArg0();
						for (InnerDb db : dbMap.values()) {
							killed = db.killSession(sessionId);
							if(killed){
								break;
							}
						}
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
    final Digest digest = MD5FileUtil.computeAndSaveDigestForFile(snapshotFile);
    final FileInfo info = new FileInfo(snapshotFile.toPath(), digest);
    fileInfos.add(info);
    for (InnerDb innerDb : dbMap.values()) {
      List<FileInfo> infos = innerDb.takeSnapshot(storage, last);
      if(!infos.isEmpty()){
        fileInfos.addAll(infos);
      }
    }
    LOG.info("Taking a DBS snapshot use time: {}", stopwatch);
    return fileInfos;
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
        Stopwatch stopwatch = Stopwatch.createStarted();
        if(snapshotFile.exists()){
          loadDbs(snapshotFile);
        }
        for (InnerDb innerDb : dbMap.values()) {
          innerDb.restoreFromSnapshot(snapshot);
        }
        LOG.info("restore DBS snapshot use time: {}", stopwatch);
      }
    }
  }


  @Override
  public void close() throws IOException {
    for (InnerDb innerDb : dbMap.values()) {
      innerDb.close();
    }
    dbMap.clear();
		this.pauseLastTime.stop();
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
