
package net.xdob.ratly.server.config;

import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.SizeInBytes;
import net.xdob.ratly.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static net.xdob.ratly.conf.ConfUtils.*;

public interface RaftServerConfigKeys {
  Logger LOG = LoggerFactory.getLogger(RaftServerConfigKeys.class);
  static Consumer<String> getDefaultLog() {
    return LOG::info;
  }

  String PREFIX = "raft.server";

  String STORAGE_DIR_KEY = PREFIX + ".storage.dir";
  List<File> STORAGE_DIR_DEFAULT = Collections.singletonList(new File("/tmp/raft-server/"));
  static List<File> storageDir(RaftProperties properties) {
    return getFiles(properties::getFiles, STORAGE_DIR_KEY, STORAGE_DIR_DEFAULT, getDefaultLog());
  }
  static void setStorageDir(RaftProperties properties, List<File> storageDir) {
    setFiles(properties::setFiles, STORAGE_DIR_KEY, storageDir);
  }

  String STORAGE_FREE_SPACE_MIN_KEY = PREFIX + ".storage.free-space.min";
  SizeInBytes STORAGE_FREE_SPACE_MIN_DEFAULT = SizeInBytes.valueOf("0MB");
  static SizeInBytes storageFreeSpaceMin(RaftProperties properties) {
    return getSizeInBytes(properties::getSizeInBytes,
        STORAGE_FREE_SPACE_MIN_KEY, STORAGE_FREE_SPACE_MIN_DEFAULT, getDefaultLog());
  }
  static void setStorageFreeSpaceMin(RaftProperties properties, SizeInBytes storageFreeSpaceMin) {
    setSizeInBytes(properties::set, STORAGE_FREE_SPACE_MIN_KEY, storageFreeSpaceMin);
  }

  String REMOVED_GROUPS_DIR_KEY = PREFIX + ".removed.groups.dir";
  File REMOVED_GROUPS_DIR_DEFAULT = new File("/tmp/raft-server/removed-groups/");
  static File removedGroupsDir(RaftProperties properties) {
    return getFile(properties::getFile, REMOVED_GROUPS_DIR_KEY,
        REMOVED_GROUPS_DIR_DEFAULT, getDefaultLog());
  }
  static void setRemovedGroupsDir(RaftProperties properties, File removedGroupsStorageDir) {
    setFile(properties::setFile, REMOVED_GROUPS_DIR_KEY, removedGroupsStorageDir);
  }

  String SLEEP_DEVIATION_THRESHOLD_KEY = PREFIX + ".sleep.deviation.threshold";
  TimeDuration SLEEP_DEVIATION_THRESHOLD_DEFAULT = TimeDuration.valueOf(300, TimeUnit.MILLISECONDS);
  static TimeDuration sleepDeviationThreshold(RaftProperties properties) {
    return getTimeDuration(properties.getTimeDuration(SLEEP_DEVIATION_THRESHOLD_DEFAULT.getUnit()),
        SLEEP_DEVIATION_THRESHOLD_KEY, SLEEP_DEVIATION_THRESHOLD_DEFAULT, getDefaultLog());
  }
  /** @deprecated use {@link #setSleepDeviationThreshold(RaftProperties, TimeDuration)}. */
  @Deprecated
  static void setSleepDeviationThreshold(RaftProperties properties, int thresholdMs) {
    setInt(properties::setInt, SLEEP_DEVIATION_THRESHOLD_KEY, thresholdMs);
  }
  static void setSleepDeviationThreshold(RaftProperties properties, TimeDuration threshold) {
    setTimeDuration(properties::setTimeDuration, SLEEP_DEVIATION_THRESHOLD_KEY, threshold);
  }

  String CLOSE_THRESHOLD_KEY = PREFIX + ".close.threshold";
  TimeDuration CLOSE_THRESHOLD_DEFAULT = TimeDuration.valueOf(60, TimeUnit.SECONDS);
  static TimeDuration closeThreshold(RaftProperties properties) {
    return getTimeDuration(properties.getTimeDuration(CLOSE_THRESHOLD_DEFAULT.getUnit()),
        CLOSE_THRESHOLD_KEY, CLOSE_THRESHOLD_DEFAULT, getDefaultLog());
  }
  /** @deprecated use {@link #setCloseThreshold(RaftProperties, TimeDuration)}. */
  @Deprecated
  static void setCloseThreshold(RaftProperties properties, int thresholdSec) {
    setInt(properties::setInt, CLOSE_THRESHOLD_KEY, thresholdSec);
  }
  static void setCloseThreshold(RaftProperties properties, TimeDuration threshold) {
    setTimeDuration(properties::setTimeDuration, CLOSE_THRESHOLD_KEY, threshold);
  }

  /**
   * When bootstrapping a new peer, If the gap between the match index of the
   * peer and the leader's latest committed index is less than this gap, we
   * treat the peer as caught-up.
   */
  String STAGING_CATCHUP_GAP_KEY = PREFIX + ".staging.catchup.gap";
  int STAGING_CATCHUP_GAP_DEFAULT = 1000; // increase this number when write throughput is high
  static int stagingCatchupGap(RaftProperties properties) {
    return getInt(properties::getInt,
        STAGING_CATCHUP_GAP_KEY, STAGING_CATCHUP_GAP_DEFAULT, getDefaultLog(), requireMin(0));
  }
  static void setStagingCatchupGap(RaftProperties properties, int stagingCatchupGap) {
    setInt(properties::setInt, STAGING_CATCHUP_GAP_KEY, stagingCatchupGap);
  }

  static void main(String[] args) {
    printAll(RaftServerConfigKeys.class);
  }
}
