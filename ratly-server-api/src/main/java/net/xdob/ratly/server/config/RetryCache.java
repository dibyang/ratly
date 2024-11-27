package net.xdob.ratly.server.config;

import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.util.TimeDuration;

import java.util.concurrent.TimeUnit;

import static net.xdob.ratly.conf.ConfUtils.getTimeDuration;
import static net.xdob.ratly.conf.ConfUtils.setTimeDuration;

/**
 * server retry cache related
 */
public interface RetryCache {
  String PREFIX = RaftServerConfigKeys.PREFIX + ".retrycache";

  /**
   * We should set expiry time longer than total client retry to guarantee exactly-once semantic
   */
  String EXPIRY_TIME_KEY = PREFIX + ".expirytime";
  TimeDuration EXPIRY_TIME_DEFAULT = TimeDuration.valueOf(60, TimeUnit.SECONDS);

  static TimeDuration expiryTime(RaftProperties properties) {
    return getTimeDuration(properties.getTimeDuration(EXPIRY_TIME_DEFAULT.getUnit()),
        EXPIRY_TIME_KEY, EXPIRY_TIME_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setExpiryTime(RaftProperties properties, TimeDuration expiryTime) {
    setTimeDuration(properties::setTimeDuration, EXPIRY_TIME_KEY, expiryTime);
  }

  String STATISTICS_EXPIRY_TIME_KEY = PREFIX + ".statistics.expirytime";
  TimeDuration STATISTICS_EXPIRY_TIME_DEFAULT = TimeDuration.valueOf(100, TimeUnit.MILLISECONDS);

  static TimeDuration statisticsExpiryTime(RaftProperties properties) {
    return getTimeDuration(properties.getTimeDuration(STATISTICS_EXPIRY_TIME_DEFAULT.getUnit()),
        STATISTICS_EXPIRY_TIME_KEY, STATISTICS_EXPIRY_TIME_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setStatisticsExpiryTime(RaftProperties properties, TimeDuration expiryTime) {
    setTimeDuration(properties::setTimeDuration, STATISTICS_EXPIRY_TIME_KEY, expiryTime);
  }
}
