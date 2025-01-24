package net.xdob.ratly.jdbc;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import net.xdob.ratly.io.MD5Hash;
import net.xdob.ratly.jdbc.exception.NoDatabaseException;
import net.xdob.ratly.json.Jsons;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.SerialSupport;
import net.xdob.ratly.security.crypto.password.PasswordEncoder;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.storage.FileInfo;
import net.xdob.ratly.server.storage.RaftStorage;
import net.xdob.ratly.server.util.RsaHelper;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.statemachine.impl.FileListStateMachineStorage;
import net.xdob.ratly.statemachine.impl.SMPlugin;
import net.xdob.ratly.statemachine.impl.SMPluginContext;
import net.xdob.ratly.util.AtomicFileOutputStream;
import net.xdob.ratly.util.FileUtils;
import net.xdob.ratly.util.MD5FileUtil;
import net.xdob.ratly.util.N3Map;

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


  private final RsaHelper rsaHelper = new RsaHelper();

  private Path dbStore;
  private SMPluginContext context;

  private final Map<String,InnerDb> dbMap = Maps.newConcurrentMap();

  private final List<DbDef> dbDefs = new ArrayList<>();

  private DbsContext dbsContext;


  public DBSMPlugin() {
  }

  @Override
  public String getId() {
    return DB;
  }

  @Override
  public void setSMPluginContext(SMPluginContext context) {
    this.context = context;
    this.dbsContext = new DbsContext() {
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
    };
  }

  @Override
  public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {

    /**
     * 初始化数据库
     */
    this.dbStore = Paths.get(raftStorage.getStorageDir().getRoot().getPath(), "db");
    if(!dbStore.toFile().exists()){
      dbStore.toFile().mkdirs();
    }
    File dbsFile = this.dbStore.resolve(DBS_JSON).toFile();
    loadDbs(dbsFile, false);

    for (DbDef dbDef : dbDefs) {
      dbMap.computeIfAbsent(dbDef.getDb(), n -> {
        DbInfo dbInfo = new DbInfo().setName(dbDef.getDb());
        String encode = context.getPasswordEncoder().encode(dbDef.getPassword());
        dbInfo.getUsers().add(new DbUser(dbDef.getUser()).setPassword(encode));
        InnerDb innerDb = new InnerDb(dbStore, dbInfo, dbsContext);
        innerDb.initialize();
        return innerDb;
      });
    }

    saveDbs();

  }

  private void loadDbs(File dbsFile, boolean loadAppliedIndex) throws IOException {
    if(dbsFile.exists()&& dbsFile.length()>0){
      List<DbState> dbs = new ArrayList<>();
      try(BufferedReader br = new BufferedReader(new InputStreamReader(
          FileUtils.newInputStream(dbsFile), StandardCharsets.UTF_8))){
        N3Map map = Jsons.i.fromJson(br, N3Map.class);
        List<DbState> dbStates = map.getValues(DbState.class, DBS);
        dbs.addAll(dbStates);
      }
      synchronized (dbMap) {
        for (DbState dbState : dbs) {
          InnerDb innerDb = dbMap.computeIfAbsent(dbState.getName(), n -> new InnerDb(dbStore, dbState.toDbInfo(), dbsContext));
          innerDb.initialize();
          if (loadAppliedIndex) {
            innerDb.updateAppliedIndexToMax(dbState.getAppliedIndex());
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
      Sender sender = queryRequest.getSender();
      QueryType type = queryRequest.getType();
      if(Sender.connection.equals(sender)&&QueryType.check.equals(type)) {
        String user = queryRequest.getUser();
        String password = rsaHelper.decrypt(queryRequest.getPassword());
        DbUser dbUser = innerDb.getDbInfo().getUser(user).orElse(null);
        if (dbUser == null) {
          throw new SQLInvalidAuthorizationSpecException();
        } else {
          PasswordEncoder passwordEncoder = context.getPasswordEncoder();
          if (!passwordEncoder.matches(password, dbUser.getPassword())) {
            throw new SQLInvalidAuthorizationSpecException();
          } else {
            if (passwordEncoder.upgradeEncoding(dbUser.getPassword())) {
              dbUser.setPassword(passwordEncoder.encode(password));
              saveDbs();
            }
          }
        }
      }
      innerDb.query(queryRequest, queryReply);
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
    if(innerDb!=null){
      innerDb.applyTransaction(updateRequest, termIndex, updateReply);
    } else {
      throw new NoDatabaseException(db);
    }
    return updateReply;
  }

  public DBSMPlugin addDbIfAbsent(String db, String user, String password){
    dbDefs.add(new DbDef(db, user, password));
    return this;
  }


  @Override
  public List<FileInfo> takeSnapshot(FileListStateMachineStorage storage, TermIndex last) throws IOException {
    Stopwatch stopwatch = Stopwatch.createStarted();
    ArrayList<FileInfo> fileInfos = Lists.newArrayList();

    final File snapshotFile =  storage.getSnapshotFile(DBS_JSON, last.getTerm(), last.getIndex());
    saveDbsToFile(snapshotFile,true);
    final MD5Hash md5 = MD5FileUtil.computeAndSaveMd5ForFile(snapshotFile);
    final FileInfo info = new FileInfo(snapshotFile.toPath(), md5);
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
      final MD5Hash md5 = MD5FileUtil.computeMd5ForFile(snapshotFile);
      if (md5.equals(fileInfo.getFileDigest())) {
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
