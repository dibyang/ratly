package net.xdob.ratly.rmap;

import com.google.protobuf.ByteString;
import net.xdob.ratly.io.Digest;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.rmap.exception.NotFindCacheException;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.raftlog.RaftLogIndex;
import net.xdob.ratly.server.storage.FileInfo;
import net.xdob.ratly.server.storage.RaftStorage;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.statemachine.impl.FileListStateMachineStorage;
import net.xdob.ratly.statemachine.impl.SMPlugin;
import net.xdob.ratly.statemachine.impl.SMPluginContext;
import net.xdob.ratly.util.AtomicFileOutputStream;
import net.xdob.ratly.util.MD5FileUtil;
import net.xdob.ratly.util.Types2;

import java.io.File;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

public class RMapSMPlugin implements SMPlugin {

  public static final String RMAP = "rmap";
  public static final String RMAP_KEY = "__rmap_key";
  public static final String APPLIED_INDEX = "appliedIndex";

  final Map<String, CacheObject> cache = Collections.synchronizedMap(new HashMap<>());
  private final RaftLogIndex appliedIndex =new RaftLogIndex("RmapAppliedIndex", RaftLog.INVALID_LOG_INDEX);
  private SMPluginContext context;


  @Override
  public String getId() {
    return RMAP;
  }


  @Override
  public void initialize(RaftServer server, RaftGroupId groupId, RaftPeerId peerId,  RaftStorage raftStorage) throws IOException {
    restoreFromSnapshot(context.getLatestSnapshot());
  }

  @Override
  public void setSMPluginContext(SMPluginContext context) {
    this.context = context;
  }

  @Override
  public void reinitialize() throws IOException {
    cache.clear();
  }

  @Override
  public Object query(Message request) {
    GetReply getReply =  new GetReply();
    try {
      GetRequest getRequest = context.as(request.getContent());
      String name = getRequest.getName();
      if(getRequest.getMethod().equals(GetMethod.size)){
        if(getRequest.getName()==null||getRequest.getName().isEmpty()){
          getReply.setSize(cache.size());
        }else {
          CacheObject cacheObject = cache.get(name);
          if(cacheObject==null){
            throw new NotFindCacheException(name);
          }
          getReply.setSize(cacheObject.size());
        }
      }
      if(getRequest.getMethod().equals(GetMethod.get)){
        if(name.isEmpty()){
          List<CacheInfo> list = cache.values().stream()
              .map(CacheObject::toCacheInfo)
              .collect(Collectors.toList());
          getReply.setData(list);
        }else {
          CacheObject cacheObject = cache.get(name);
          if (cacheObject == null) {
            throw new NotFindCacheException(name);
          }
          if (getRequest.getFun()!=null) {
            Object value = getRequest.getFun().apply(cacheObject);
            getReply.setData(value);
          } else if( getRequest.getKey()!=null) {
            Object value = cacheObject.getMap().get(getRequest.getKey());
            getReply.setData(value);
          }else{
            getReply.setData(cacheObject.getMap());
          }
        }
      }
    } catch (IOException e) {
      getReply.setEx(e);
    }
    return getReply;
  }

  @Override
  public Object applyTransaction(TermIndex termIndex, ByteString data) {
    PutReply putReply = new PutReply();

    try {
      PutRequest putRequest = context.as(data);
      String name = putRequest.getName();
      String key = putRequest.getKey();
      if(putRequest.getMethod().equals(PutMethod.create)){
        CacheInfo cacheInfo = cache.computeIfAbsent(name, n->CacheObject.newMap(n))
            .toCacheInfo();
        putReply.setData(cacheInfo);
      } else if(putRequest.getMethod().equals(PutMethod.drop)){
        CacheObject remove = cache.remove(name);
        if(remove!=null){
          putReply.setData(remove.toCacheInfo());
        }
      }else if(putRequest.getMethod().equals(PutMethod.remove)){
        CacheObject cacheObject = cache.get(name);
        if(cacheObject!=null){
          Object remove = cacheObject.getMap().remove(key);
          putReply.setData(remove);
        }
      }else if(putRequest.getMethod().equals(PutMethod.put)){
        CacheObject cacheObject = cache.get(name);
        if (cacheObject == null) {
          throw new NotFindCacheException(name);
        }else {
          if(putRequest.getFun()!=null){
            Object value = putRequest.getFun().apply(cacheObject, putRequest.getData());
            putReply.setData(value);
          }else if(putRequest.getData()!=null){
            Object value = putRequest.getData();
            cacheObject.getMap().put(key, value);
          }
        }
      }
      updateAppliedIndexToMax(termIndex.getIndex());
    } catch (IOException e) {
      putReply.setEx(e);
    }
    return putReply;
  }

  private void updateAppliedIndexToMax(long index) {
    appliedIndex.updateToMax(index,
        message -> LOG.debug("updateAppliedIndex {}", message));
  }


  @Override
  public List<FileInfo> takeSnapshot(FileListStateMachineStorage storage, TermIndex last) throws IOException {
    final File snapshotFile =  storage.getSnapshotFile(RMAP, last.getTerm(), last.getIndex());
    SMPlugin.LOG.info("Taking a snapshot to file {}", snapshotFile);
    CacheObject cacheObject = cache.computeIfAbsent(RMAP_KEY, CacheObject::new);
    cacheObject.getMap().put(APPLIED_INDEX, appliedIndex.get());
    try (FilterOutputStream out = new AtomicFileOutputStream(snapshotFile)) {
      byte[] bytes = context.getFasts().asBytes(cache);
      out.write(bytes);
    }
    final Digest digest = MD5FileUtil.computeAndSaveDigestForFile(snapshotFile);
    final FileInfo info = new FileInfo(snapshotFile.toPath(), digest);
    return Arrays.asList(info);
  }

  @Override
  public void restoreFromSnapshot(SnapshotInfo snapshot) throws IOException {
    if(snapshot==null){
      return;
    }
    List<FileInfo> files = snapshot.getFiles(RMAP);
    if(!files.isEmpty()) {
      FileInfo fileInfo = files.get(0);
      final File snapshotFile = fileInfo.getPath().toFile();
      final String snapshotFileName = snapshotFile.getPath();
      SMPlugin.LOG.info("restore map snapshot from {}", snapshotFileName);
      final Digest digest = MD5FileUtil.computeAndSaveDigestForFile(snapshotFile);
      if (digest.equals(fileInfo.getFileDigest())) {
        byte[] bytes = Files.readAllBytes(snapshotFile.toPath());
        Map<String,CacheObject> old =(Map<String,CacheObject>)context.getFasts().asObject(bytes);
        CacheObject removed = old.remove(RMAP_KEY);
        if(removed!=null){
          Types2.cast(removed.getMap().get(APPLIED_INDEX),Long.class)
              .ifPresent(this::updateAppliedIndexToMax);
        }
        cache.clear();
        cache.putAll(old);
      }
    }
  }



  @Override
  public void close() throws IOException {

  }
}
