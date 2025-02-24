package net.xdob.ratly.so;

import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import net.xdob.ratly.server.RatlyLogTracer;
import net.xdob.ratly.server.StateObserver;
import net.xdob.ratly.server.TermLeader;
import net.xdob.ratly.util.AtomicFileOutputStream;
import net.xdob.ratly.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class StateObserver4fs implements StateObserver {
  static final Splitter SPLITTER = Splitter.on(" ").trimResults().omitEmptyStrings();
  public static final int DEFAULT_TIMEOUT = 500;
  /**
   * 挂载点配置key
   */
  public static final String RAFT_OS4FS_MOUNT = "raft.os4fs.mount";
  /**
   * 默认挂载点
   */
  public static final String DEFAULT_MOUNT = "/datapool";
  public static final String DIR_PATH = "/.so";
  public static final String EXT = ".token";

  static Logger LOG = LoggerFactory.getLogger(StateObserver4fs.class);

  private final AtomicReference<String> groupIdRef = new AtomicReference<>();
  private final AtomicReference<TermLeader> termLeaderRef = new AtomicReference<>();
  private volatile ScheduledExecutorService scheduled;



  @Override
  public boolean isStarted() {
    return (scheduled != null);
  }

  @Override
  public void stop() {
    scheduled = null;
  }

  @Override
  public String getName() {
    return "so4fs";
  }

  @Override
  public void start(ScheduledExecutorService scheduled) {
    this.scheduled = scheduled;

  }

  @Override
  public CompletableFuture<TermLeader> getLastLeaderTerm(String groupId, int waitMS) {
    groupIdRef.compareAndSet(null, groupId);
    CompletableFuture<TermLeader> future = CompletableFuture.supplyAsync(() -> {
      String mount = getMount();
      if (isMount(mount)) {
        File file = Paths.get(mount, DIR_PATH, groupId + EXT).toFile();
        if(!file.exists()){
          file = Paths.get(mount, DIR_PATH, groupId + ".pro").toFile();
        }
        try (InputStream in = FileUtils.newInputStream(file)) {
          Properties properties = new Properties();
          properties.load(in);
          TermLeader termLeader = TermLeader.of(getTerm(properties), getLeader(properties));
          termLeader.setIndex(getIndex(properties));
          return termLeader;
        } catch (IOException e) {
          LOG.warn("load file err, path={}", file, e);
        }
      }
      return null;
    });
    scheduled.schedule(()->future.cancel(true), getTimeout(), TimeUnit.MILLISECONDS);
    return future;
  }

  private int getTimeout() {
    return DEFAULT_TIMEOUT;
  }

  static String getLeader(Properties properties) throws IOException {
    try {
      return getValue(LEADER, properties);
    } catch (Exception e) {
      throw new IOException("Failed to parse '" + LEADER + "' from properties: " + properties, e);
    }
  }

  static long getTerm(Properties properties) throws IOException {
    try {
      return Long.parseLong(getValue(TERM, properties));
    } catch (Exception e) {
      throw new IOException("Failed to parse '" + TERM + "' from properties: " + properties, e);
    }
  }

  static long getIndex(Properties properties) throws IOException {
    try {
      return Long.parseLong(getValue(INDEX, properties));
    } catch (Exception e) {
      throw new IOException("Failed to parse '" + INDEX + "' from properties: " + properties, e);
    }
  }

  static String getValue(String key, Properties properties) throws IOException {
    return Optional.ofNullable(properties.getProperty(key)).orElseThrow(
        () -> new IOException("'" + key + "' not found in properties: " + properties));
  }

  @Override
  public void notifyTeamIndex(String groupId, TermLeader termLeader) {
    groupIdRef.compareAndSet(null, groupId);
    TermLeader updated = termLeaderRef.updateAndGet(old -> {
      if (old != null && old.getIndex() > termLeader.getIndex()) {
        return old;
      }
      return termLeader;
    });
    if(termLeader.equals(updated)) {
      CompletableFuture<Void> future = CompletableFuture.runAsync(this::writeTeamIndex);
      scheduled.schedule(() -> future.cancel(true), getTimeout(), TimeUnit.MILLISECONDS);
    }
  }

  private  void writeTeamIndex() {
    String groupId = groupIdRef.get();
    if(groupId==null)return;
    String mount = getMount();
    if (isMount(mount)) {
      File file = Paths.get(mount, DIR_PATH, groupId + EXT).toFile();
      synchronized (groupId){
        try {
          if(!file.getParentFile().exists()){
            file.getParentFile().mkdirs();
          }
          TermLeader leader = termLeaderRef.get();
          if(leader!=null) {
            Properties properties = new Properties();
            properties.put(LEADER, leader.getLeaderId());
            properties.put(TERM, String.valueOf(leader.getTerm()));
            properties.put(INDEX, String.valueOf(leader.getIndex()));
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            properties.store(bos,"");
            Stopwatch stopwatch = Stopwatch.createStarted();
            try (OutputStream out =new AtomicFileOutputStream(file)) {
              out.write(bos.toByteArray());
            }
            stopwatch.stop();
            if(RatlyLogTracer.os_cost_time.isTrace()) {
              LOG.info("store file:{} cost time:{}", file, stopwatch);
            }
          }
        }catch (Exception e){
          LOG.warn("store file err, path={}", file, e);
        }
      }
    }
  }

  boolean isMount(String path){
    try {
      List<String> lines = Files.readAllLines(Paths.get("/proc/mounts"));
      List<String> list = lines.stream().map(e -> SPLITTER.splitToList(e).get(1))
          .collect(Collectors.toList());
      return list.contains(path);
    } catch (IOException e) {
      LOG.warn("read /proc/mounts error", e);
    }
    return false;
  }

  String getMount(){
    String mount = System.getProperty(RAFT_OS4FS_MOUNT, DEFAULT_MOUNT);
    if(mount.endsWith("/")){
      mount = mount.substring(0, mount.length()-1);
    }
    return mount;
  }

}
