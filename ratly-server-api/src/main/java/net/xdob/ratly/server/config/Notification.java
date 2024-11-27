package net.xdob.ratly.server.config;

import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.TimeDuration;

import java.util.concurrent.TimeUnit;

import static net.xdob.ratly.conf.ConfUtils.getTimeDuration;
import static net.xdob.ratly.conf.ConfUtils.setTimeDuration;

public interface Notification {
  String PREFIX = RaftServerConfigKeys.PREFIX + "." + JavaUtils.getClassSimpleName(Notification.class).toLowerCase();

  /**
   * Timeout value to notify the state machine when there is no leader.
   */
  String NO_LEADER_TIMEOUT_KEY = PREFIX + ".no-leader.timeout";
  TimeDuration NO_LEADER_TIMEOUT_DEFAULT = TimeDuration.valueOf(60, TimeUnit.SECONDS);

  static TimeDuration noLeaderTimeout(RaftProperties properties) {
    return getTimeDuration(properties.getTimeDuration(NO_LEADER_TIMEOUT_DEFAULT.getUnit()),
        NO_LEADER_TIMEOUT_KEY, NO_LEADER_TIMEOUT_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setNoLeaderTimeout(RaftProperties properties, TimeDuration noLeaderTimeout) {
    setTimeDuration(properties::setTimeDuration, NO_LEADER_TIMEOUT_KEY, noLeaderTimeout);
  }
}
