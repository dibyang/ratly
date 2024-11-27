package net.xdob.ratly.server.config;

import net.xdob.ratly.conf.ConfUtils;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.TimeDuration;

import java.util.concurrent.TimeUnit;

import static net.xdob.ratly.conf.ConfUtils.*;

public interface Read {
  String PREFIX = RaftServerConfigKeys.PREFIX
      + "." + JavaUtils.getClassSimpleName(Read.class).toLowerCase();

  String TIMEOUT_KEY = PREFIX + ".timeout";
  TimeDuration TIMEOUT_DEFAULT = TimeDuration.valueOf(10, TimeUnit.SECONDS);

  static TimeDuration timeout(RaftProperties properties) {
    return ConfUtils.getTimeDuration(properties.getTimeDuration(TIMEOUT_DEFAULT.getUnit()),
        TIMEOUT_KEY, TIMEOUT_DEFAULT, RaftServerConfigKeys.getDefaultLog(), requirePositive());
  }

  static void setTimeout(RaftProperties properties, TimeDuration readOnlyTimeout) {
    setTimeDuration(properties::setTimeDuration, TIMEOUT_KEY, readOnlyTimeout);
  }

  enum Option {
    /**
     * Directly query statemachine. Efficient but may undermine linearizability
     */
    DEFAULT,

    /**
     * Use ReadIndex (see Raft Paper section 6.4). Maintains linearizability
     */
    LINEARIZABLE
  }

  String OPTION_KEY = PREFIX + ".option";
  Option OPTION_DEFAULT = Option.DEFAULT;

  static Option option(RaftProperties properties) {
    Option option = get(properties::getEnum, OPTION_KEY, OPTION_DEFAULT, RaftServerConfigKeys.getDefaultLog());
    if (option != Option.DEFAULT && option != Option.LINEARIZABLE) {
      throw new IllegalArgumentException("Unexpected read option: " + option);
    }
    return option;
  }

  static void setOption(RaftProperties properties, Option option) {
    set(properties::setEnum, OPTION_KEY, option);
  }

  String LEADER_LEASE_ENABLED_KEY = PREFIX + ".leader.lease.enabled";
  boolean LEADER_LEASE_ENABLED_DEFAULT = false;

  static boolean leaderLeaseEnabled(RaftProperties properties) {
    return getBoolean(properties::getBoolean, LEADER_LEASE_ENABLED_KEY,
        LEADER_LEASE_ENABLED_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setLeaderLeaseEnabled(RaftProperties properties, boolean enabled) {
    setBoolean(properties::setBoolean, LEADER_LEASE_ENABLED_KEY, enabled);
  }

  String LEADER_LEASE_TIMEOUT_RATIO_KEY = PREFIX + ".leader.lease.timeout.ratio";
  double LEADER_LEASE_TIMEOUT_RATIO_DEFAULT = 0.9;

  static double leaderLeaseTimeoutRatio(RaftProperties properties) {
    return getDouble(properties::getDouble, LEADER_LEASE_TIMEOUT_RATIO_KEY,
        LEADER_LEASE_TIMEOUT_RATIO_DEFAULT, RaftServerConfigKeys.getDefaultLog(),
        requireMin(0.0), requireMax(1.0));
  }

  static void setLeaderLeaseTimeoutRatio(RaftProperties properties, double ratio) {
    setDouble(properties::setDouble, LEADER_LEASE_TIMEOUT_RATIO_KEY, ratio);
  }

  interface ReadAfterWriteConsistent {
    String PREFIX = Read.PREFIX + ".read-after-write-consistent";

    String WRITE_INDEX_CACHE_EXPIRY_TIME_KEY = PREFIX + ".write-index-cache.expiry-time";
    /**
     * Must be larger than {@link Read#TIMEOUT_DEFAULT}.
     */
    TimeDuration WRITE_INDEX_CACHE_EXPIRY_TIME_DEFAULT = TimeDuration.valueOf(60, TimeUnit.SECONDS);

    static TimeDuration writeIndexCacheExpiryTime(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(WRITE_INDEX_CACHE_EXPIRY_TIME_DEFAULT.getUnit()),
          WRITE_INDEX_CACHE_EXPIRY_TIME_KEY, WRITE_INDEX_CACHE_EXPIRY_TIME_DEFAULT, RaftServerConfigKeys.getDefaultLog());
    }

    static void setWriteIndexCacheExpiryTime(RaftProperties properties, TimeDuration expiryTime) {
      setTimeDuration(properties::setTimeDuration, WRITE_INDEX_CACHE_EXPIRY_TIME_KEY, expiryTime);
    }
  }
}
