package net.xdob.ratly.server.config;

import net.xdob.ratly.conf.ConfUtils;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.util.TimeDuration;

import java.util.concurrent.TimeUnit;

import static net.xdob.ratly.conf.ConfUtils.*;
import static net.xdob.ratly.conf.ConfUtils.getTimeDuration;

public interface Watch {
  String PREFIX = RaftServerConfigKeys.PREFIX + ".watch";

  String ELEMENT_LIMIT_KEY = PREFIX + ".element-limit";
  int ELEMENT_LIMIT_DEFAULT = 65536;

  static int elementLimit(RaftProperties properties) {
    return ConfUtils.getInt(properties::getInt, ELEMENT_LIMIT_KEY, ELEMENT_LIMIT_DEFAULT, RaftServerConfigKeys.getDefaultLog(), requireMin(1));
  }

  static void setElementLimit(RaftProperties properties, int limit) {
    setInt(properties::setInt, ELEMENT_LIMIT_KEY, limit, requireMin(1));
  }

  String TIMEOUT_DENOMINATION_KEY = PREFIX + ".timeout.denomination";
  TimeDuration TIMEOUT_DENOMINATION_DEFAULT = TimeDuration.valueOf(1, TimeUnit.SECONDS);

  static TimeDuration timeoutDenomination(RaftProperties properties) {
    return ConfUtils.getTimeDuration(properties.getTimeDuration(TIMEOUT_DENOMINATION_DEFAULT.getUnit()),
        TIMEOUT_DENOMINATION_KEY, TIMEOUT_DENOMINATION_DEFAULT, RaftServerConfigKeys.getDefaultLog(), requirePositive());
  }

  static void setTimeoutDenomination(RaftProperties properties, TimeDuration watchTimeout) {
    setTimeDuration(properties::setTimeDuration, TIMEOUT_DENOMINATION_KEY, watchTimeout);
  }

  /**
   * Timeout for watch requests.
   */
  String TIMEOUT_KEY = PREFIX + ".timeout";
  TimeDuration TIMEOUT_DEFAULT = TimeDuration.valueOf(10, TimeUnit.SECONDS);

  static TimeDuration timeout(RaftProperties properties) {
    return ConfUtils.getTimeDuration(properties.getTimeDuration(TIMEOUT_DEFAULT.getUnit()),
        TIMEOUT_KEY, TIMEOUT_DEFAULT, RaftServerConfigKeys.getDefaultLog(), requirePositive());
  }

  static void setTimeout(RaftProperties properties, TimeDuration watchTimeout) {
    setTimeDuration(properties::setTimeDuration, TIMEOUT_KEY, watchTimeout);
  }
}
