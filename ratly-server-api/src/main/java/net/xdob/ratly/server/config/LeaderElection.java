package net.xdob.ratly.server.config;

import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.TimeDuration;

import java.util.concurrent.TimeUnit;

import static net.xdob.ratly.conf.ConfUtils.*;

public interface LeaderElection {
  String PREFIX = RaftServerConfigKeys.PREFIX
      + "." + JavaUtils.getClassSimpleName(LeaderElection.class).toLowerCase();

  String LEADER_STEP_DOWN_WAIT_TIME_KEY = PREFIX + ".leader.step-down.wait-time";
  TimeDuration LEADER_STEP_DOWN_WAIT_TIME_DEFAULT = TimeDuration.valueOf(10, TimeUnit.SECONDS);

  static TimeDuration leaderStepDownWaitTime(RaftProperties properties) {
    return getTimeDuration(properties.getTimeDuration(LEADER_STEP_DOWN_WAIT_TIME_DEFAULT.getUnit()),
        LEADER_STEP_DOWN_WAIT_TIME_KEY, LEADER_STEP_DOWN_WAIT_TIME_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setLeaderStepDownWaitTime(RaftProperties properties, TimeDuration leaderStepDownWaitTime) {
    setTimeDuration(properties::setTimeDuration, LEADER_STEP_DOWN_WAIT_TIME_KEY, leaderStepDownWaitTime);
  }

  String PRE_VOTE_KEY = PREFIX + ".pre-vote";
  boolean PRE_VOTE_DEFAULT = true;

  static boolean preVote(RaftProperties properties) {
    return getBoolean(properties::getBoolean, PRE_VOTE_KEY, PRE_VOTE_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setPreVote(RaftProperties properties, boolean enablePreVote) {
    setBoolean(properties::setBoolean, PRE_VOTE_KEY, enablePreVote);
  }

  /**
   * Does it allow majority-add, i.e. adding a majority of members in a single setConf?
   * <p>
   * Note that, when a single setConf removes and adds members at the same time,
   * the majority is counted after the removal.
   * For examples, setConf to a 3-member group by adding 2 new members is NOT a majority-add.
   * However, setConf to a 3-member group by removing 2 of members and adding 2 new members is a majority-add.
   * <p>
   * Note also that adding 1 new member to an 1-member group is always allowed,
   * although it is a majority-add.
   */
  String MEMBER_MAJORITY_ADD_KEY = PREFIX + ".member.majority-add";
  boolean MEMBER_MAJORITY_ADD_DEFAULT = false;

  static boolean memberMajorityAdd(RaftProperties properties) {
    return getBoolean(properties::getBoolean, MEMBER_MAJORITY_ADD_KEY, MEMBER_MAJORITY_ADD_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setMemberMajorityAdd(RaftProperties properties, boolean enableMemberMajorityAdd) {
    setBoolean(properties::setBoolean, MEMBER_MAJORITY_ADD_KEY, enableMemberMajorityAdd);
  }
}
