package net.xdob.ratly.server.config;

import net.xdob.ratly.conf.ConfUtils;
import net.xdob.ratly.conf.RaftProperties;

import static net.xdob.ratly.conf.ConfUtils.*;

public interface Snapshot {
  String PREFIX = RaftServerConfigKeys.PREFIX + ".snapshot";

  /**
   * whether trigger snapshot when log size exceeds limit
   */
  String AUTO_TRIGGER_ENABLED_KEY = PREFIX + ".auto.trigger.enabled";
  /**
   * by default let the state machine to decide when to do checkpoint
   */
  boolean AUTO_TRIGGER_ENABLED_DEFAULT = false;

  static boolean autoTriggerEnabled(RaftProperties properties) {
    return getBoolean(properties::getBoolean,
        AUTO_TRIGGER_ENABLED_KEY, AUTO_TRIGGER_ENABLED_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setAutoTriggerEnabled(RaftProperties properties, boolean autoTriggerEnabled) {
    setBoolean(properties::setBoolean, AUTO_TRIGGER_ENABLED_KEY, autoTriggerEnabled);
  }

  /**
   * whether trigger snapshot when stop raft server
   */
  String TRIGGER_WHEN_STOP_ENABLED_KEY = PREFIX + ".trigger-when-stop.enabled";
  /**
   * by default let the state machine to trigger snapshot when stop
   */
  boolean TRIGGER_WHEN_STOP_ENABLED_DEFAULT = true;

  static boolean triggerWhenStopEnabled(RaftProperties properties) {
    return getBoolean(properties::getBoolean,
        TRIGGER_WHEN_STOP_ENABLED_KEY, TRIGGER_WHEN_STOP_ENABLED_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setTriggerWhenStopEnabled(RaftProperties properties, boolean triggerWhenStopEnabled) {
    setBoolean(properties::setBoolean, TRIGGER_WHEN_STOP_ENABLED_KEY, triggerWhenStopEnabled);
  }

  /**
   * whether trigger snapshot when remove raft server
   */
  String TRIGGER_WHEN_REMOVE_ENABLED_KEY = PREFIX + ".trigger-when-remove.enabled";
  /**
   * by default let the state machine to trigger snapshot when remove
   */
  boolean TRIGGER_WHEN_REMOVE_ENABLED_DEFAULT = true;

  static boolean triggerWhenRemoveEnabled(RaftProperties properties) {
    return getBoolean(properties::getBoolean,
        TRIGGER_WHEN_REMOVE_ENABLED_KEY, TRIGGER_WHEN_REMOVE_ENABLED_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setTriggerWhenRemoveEnabled(RaftProperties properties, boolean triggerWhenRemoveEnabled) {
    setBoolean(properties::setBoolean, TRIGGER_WHEN_REMOVE_ENABLED_KEY, triggerWhenRemoveEnabled);
  }

  /**
   * The log index gap between to two snapshot creations.
   */
  String CREATION_GAP_KEY = PREFIX + ".creation.gap";
  long CREATION_GAP_DEFAULT = 1024;

  static long creationGap(RaftProperties properties) {
    return ConfUtils.getLong(
        properties::getLong, CREATION_GAP_KEY, CREATION_GAP_DEFAULT,
        RaftServerConfigKeys.getDefaultLog(), requireMin(1L));
  }

  static void setCreationGap(RaftProperties properties, long creationGap) {
    setLong(properties::setLong, CREATION_GAP_KEY, creationGap);
  }

  /**
   * log size limit (in number of log entries) that triggers the snapshot
   */
  String AUTO_TRIGGER_THRESHOLD_KEY = PREFIX + ".auto.trigger.threshold";
  long AUTO_TRIGGER_THRESHOLD_DEFAULT = 400000L;

  static long autoTriggerThreshold(RaftProperties properties) {
    return ConfUtils.getLong(properties::getLong,
        AUTO_TRIGGER_THRESHOLD_KEY, AUTO_TRIGGER_THRESHOLD_DEFAULT, RaftServerConfigKeys.getDefaultLog(), requireMin(0L));
  }

  static void setAutoTriggerThreshold(RaftProperties properties, long autoTriggerThreshold) {
    setLong(properties::setLong, AUTO_TRIGGER_THRESHOLD_KEY, autoTriggerThreshold);
  }

  String RETENTION_FILE_NUM_KEY = PREFIX + ".retention.file.num";
  int RETENTION_FILE_NUM_DEFAULT = -1;

  static int retentionFileNum(RaftProperties raftProperties) {
    return getInt(raftProperties::getInt, RETENTION_FILE_NUM_KEY, RETENTION_FILE_NUM_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setRetentionFileNum(RaftProperties properties, int numSnapshotFilesRetained) {
    setInt(properties::setInt, RETENTION_FILE_NUM_KEY, numSnapshotFilesRetained);
  }
}
