package net.xdob.ratly.server.config;

import net.xdob.ratly.conf.ConfUtils;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.util.TimeDuration;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static net.xdob.ratly.conf.ConfUtils.getTimeDuration;
import static net.xdob.ratly.conf.ConfUtils.setTimeDuration;

/**
 * server rpc timeout related
 */
public interface Rpc {
  String PREFIX = RaftServerConfigKeys.PREFIX + ".rpc";

  String TIMEOUT_MIN_KEY = PREFIX + ".timeout.min";
  TimeDuration TIMEOUT_MIN_DEFAULT = TimeDuration.valueOf(150, TimeUnit.MILLISECONDS);

  static TimeDuration timeoutMin(RaftProperties properties, Consumer<String> logger) {
    return getTimeDuration(properties.getTimeDuration(TIMEOUT_MIN_DEFAULT.getUnit()),
        TIMEOUT_MIN_KEY, TIMEOUT_MIN_DEFAULT, logger);
  }

  static TimeDuration timeoutMin(RaftProperties properties) {
    return timeoutMin(properties, RaftServerConfigKeys.getDefaultLog());
  }

  static void setTimeoutMin(RaftProperties properties, TimeDuration minDuration) {
    setTimeDuration(properties::setTimeDuration, TIMEOUT_MIN_KEY, minDuration);
  }

  String TIMEOUT_MAX_KEY = PREFIX + ".timeout.max";
  TimeDuration TIMEOUT_MAX_DEFAULT = TimeDuration.valueOf(300, TimeUnit.MILLISECONDS);

  static TimeDuration timeoutMax(RaftProperties properties, Consumer<String> logger) {
    return getTimeDuration(properties.getTimeDuration(TIMEOUT_MAX_DEFAULT.getUnit()),
        TIMEOUT_MAX_KEY, TIMEOUT_MAX_DEFAULT, logger);
  }

  static TimeDuration timeoutMax(RaftProperties properties) {
    return timeoutMax(properties, RaftServerConfigKeys.getDefaultLog());
  }

  static void setTimeoutMax(RaftProperties properties, TimeDuration maxDuration) {
    setTimeDuration(properties::setTimeDuration, TIMEOUT_MAX_KEY, maxDuration);
  }

  /**
   * separate first timeout so that the startup unavailable time can be reduced
   */
  String FIRST_ELECTION_TIMEOUT_MIN_KEY = PREFIX + ".first-election.timeout.min";
  TimeDuration FIRST_ELECTION_TIMEOUT_MIN_DEFAULT = null;

  static TimeDuration firstElectionTimeoutMin(RaftProperties properties) {
    final TimeDuration fallbackFirstElectionTimeoutMin = Rpc.timeoutMin(properties, null);
    return ConfUtils.getTimeDuration(properties.getTimeDuration(fallbackFirstElectionTimeoutMin.getUnit()),
        FIRST_ELECTION_TIMEOUT_MIN_KEY, FIRST_ELECTION_TIMEOUT_MIN_DEFAULT,
        Rpc.TIMEOUT_MIN_KEY, fallbackFirstElectionTimeoutMin, RaftServerConfigKeys.getDefaultLog());
  }

  static void setFirstElectionTimeoutMin(RaftProperties properties, TimeDuration firstMinDuration) {
    setTimeDuration(properties::setTimeDuration, FIRST_ELECTION_TIMEOUT_MIN_KEY, firstMinDuration);
  }

  String FIRST_ELECTION_TIMEOUT_MAX_KEY = PREFIX + ".first-election.timeout.max";
  TimeDuration FIRST_ELECTION_TIMEOUT_MAX_DEFAULT = null;

  static TimeDuration firstElectionTimeoutMax(RaftProperties properties) {
    final TimeDuration fallbackFirstElectionTimeoutMax = Rpc.timeoutMax(properties, null);
    return ConfUtils.getTimeDuration(properties.getTimeDuration(fallbackFirstElectionTimeoutMax.getUnit()),
        FIRST_ELECTION_TIMEOUT_MAX_KEY, FIRST_ELECTION_TIMEOUT_MAX_DEFAULT,
        Rpc.TIMEOUT_MAX_KEY, fallbackFirstElectionTimeoutMax, RaftServerConfigKeys.getDefaultLog());
  }

  static void setFirstElectionTimeoutMax(RaftProperties properties, TimeDuration firstMaxDuration) {
    setTimeDuration(properties::setTimeDuration, FIRST_ELECTION_TIMEOUT_MAX_KEY, firstMaxDuration);
  }

  String REQUEST_TIMEOUT_KEY = PREFIX + ".request.timeout";
  TimeDuration REQUEST_TIMEOUT_DEFAULT = TimeDuration.valueOf(3000, TimeUnit.MILLISECONDS);

  static TimeDuration requestTimeout(RaftProperties properties) {
    return getTimeDuration(properties.getTimeDuration(REQUEST_TIMEOUT_DEFAULT.getUnit()),
        REQUEST_TIMEOUT_KEY, REQUEST_TIMEOUT_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setRequestTimeout(RaftProperties properties, TimeDuration timeoutDuration) {
    setTimeDuration(properties::setTimeDuration, REQUEST_TIMEOUT_KEY, timeoutDuration);
  }

  String SLEEP_TIME_KEY = PREFIX + ".sleep.time";
  TimeDuration SLEEP_TIME_DEFAULT = TimeDuration.valueOf(25, TimeUnit.MILLISECONDS);

  static TimeDuration sleepTime(RaftProperties properties) {
    return getTimeDuration(properties.getTimeDuration(SLEEP_TIME_DEFAULT.getUnit()),
        SLEEP_TIME_KEY, SLEEP_TIME_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setSleepTime(RaftProperties properties, TimeDuration sleepTime) {
    setTimeDuration(properties::setTimeDuration, SLEEP_TIME_KEY, sleepTime);
  }

  String SLOWNESS_TIMEOUT_KEY = PREFIX + ".slowness.timeout";
  TimeDuration SLOWNESS_TIMEOUT_DEFAULT = TimeDuration.valueOf(60, TimeUnit.SECONDS);

  static TimeDuration slownessTimeout(RaftProperties properties) {
    return getTimeDuration(properties.getTimeDuration(SLOWNESS_TIMEOUT_DEFAULT.getUnit()),
        SLOWNESS_TIMEOUT_KEY, SLOWNESS_TIMEOUT_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setSlownessTimeout(RaftProperties properties, TimeDuration expiryTime) {
    setTimeDuration(properties::setTimeDuration, SLOWNESS_TIMEOUT_KEY, expiryTime);
  }
}
