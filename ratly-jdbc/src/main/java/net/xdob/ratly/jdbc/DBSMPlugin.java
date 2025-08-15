package net.xdob.ratly.jdbc;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import net.xdob.ratly.io.Digest;
import net.xdob.ratly.jdbc.exception.NoDatabaseException;
import net.xdob.ratly.json.Jsons;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.protocol.SerialSupport;
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


  private Path dbStore;
  private Path dbCache;
  private SMPluginContext context;

  private final Map<String,InnerDb> dbMap = Maps.newConcurrentMap();

  private final Map<String, DbDef> dbDefs = Maps.newConcurrentMap();;
  private final RsaHelper rsaHelper = new RsaHelper();

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
    this.dbsContext = new DbsContext() {
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
      public SerialSupport getFasts() {
        return context.getFasts();
      }

      @Override
      public boolean isLeader() {
        return context.isLeader();
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
      public void closeSession(String db, String sessionId) {
        //暂时没有实现
      }

      @Override
      public void stopServerState() {
        context.stopServerState();
      }
    };
  }

  @Override
  public void initialize(RaftServer server, RaftGroupId groupId, RaftPeerId peerId, RaftStorage raftStorage) throws IOException {

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
    loadDbs(dbsFile, false);
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

  private void loadDbs(File dbsFile, boolean loadAppliedIndex) throws IOException {
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
        if (loadAppliedIndex) {
          innerDb.updateAppliedIndexToMax(dbState.getAppliedIndex());
        }
      }
    }
  }

  @Override
  public void reinitialize() throws IOException {
  }

  private void saveDbs(){
    File dbsFile = this.dbStore.resolve(DBS_JSON).toFile();
    saveDbsToFile(dbsFile, false);
  }

  private void saveDbsToFile(File dbsFile, boolean saveAppliedIndex) {

    N3Map n3Map = new N3Map();

    List<DbInfo> dbStateList = dbMap.values().stream()
        .map(e->!saveAppliedIndex?e.getDbInfo():e.getDbState())
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
  public Object query(Message request) throws SQLException {
    QueryReply queryReply = new QueryReply();
    QueryRequest queryRequest = context.as(request.getContent());
    String db = queryRequest.getDb();
    InnerDb innerDb = dbMap.get(db);
    if(innerDb!=null){
      try {
        innerDb.query(queryRequest, queryReply);
      } catch (SQLException e) {
        queryReply.setEx(e);
      }
    } else {
      throw new NoDatabaseException(db);
    }
    return queryReply;
  }

  @Override
  public Object applyTransaction(TermIndex termIndex, ByteString msg) throws SQLException {

    UpdateReply updateReply = new UpdateReply();
    UpdateRequest updateRequest = context.as(msg);
    String db = updateRequest.getDb();
    InnerDb innerDb = dbMap.get(db);
    if(innerDb==null&&dynamicCreate
        &&(updateRequest.getType()==UpdateType.openSession)){
      String user = updateRequest.getUser();
      String password = rsaHelper.decrypt(updateRequest.getPassword());
      this.addDbIfAbsent(db, user, password);
      addDbIfAbsent(dbDefs.get(db));
      saveDbs();
      innerDb = dbMap.get(db);
    }
    if(innerDb!=null){
      innerDb.applyTransaction(updateRequest, termIndex, updateReply);
    } else{
      throw new NoDatabaseException(db);
    }
    return updateReply;
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
    saveDbsToFile(snapshotFile,true);
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
          loadDbs(snapshotFile, true);
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
  }


  @Override
  public long getLastPluginAppliedIndex() {
    Long lastDbAppliedIndex = dbMap.values().stream()
        .map(InnerDb::getLastPluginAppliedIndex)
        .filter(e-> e > RaftLog.INVALID_LOG_INDEX)
        .min(Comparator.comparingLong(e->e))
        .orElse(RaftLog.INVALID_LOG_INDEX);
    if(lastDbAppliedIndex > RaftLog.INVALID_LOG_INDEX){
      return lastDbAppliedIndex;
    }
    return RaftLog.INVALID_LOG_INDEX;
  }
}
