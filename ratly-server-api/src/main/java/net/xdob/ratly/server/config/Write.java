package net.xdob.ratly.server.config;

import net.xdob.ratly.conf.ConfUtils;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.util.SizeInBytes;

import static net.xdob.ratly.conf.ConfUtils.*;

public interface Write {
  String PREFIX = RaftServerConfigKeys.PREFIX + ".write";

  String ELEMENT_LIMIT_KEY = PREFIX + ".element-limit";
  int ELEMENT_LIMIT_DEFAULT = 4096;

  static int elementLimit(RaftProperties properties) {
    return ConfUtils.getInt(properties::getInt, ELEMENT_LIMIT_KEY, ELEMENT_LIMIT_DEFAULT, RaftServerConfigKeys.getDefaultLog(), requireMin(1));
  }

  static void setElementLimit(RaftProperties properties, int limit) {
    setInt(properties::setInt, ELEMENT_LIMIT_KEY, limit, requireMin(1));
  }

  String BYTE_LIMIT_KEY = PREFIX + ".byte-limit";
  SizeInBytes BYTE_LIMIT_DEFAULT = SizeInBytes.valueOf("64MB");

  static SizeInBytes byteLimit(RaftProperties properties) {
    return ConfUtils.getSizeInBytes(properties::getSizeInBytes,
        BYTE_LIMIT_KEY, BYTE_LIMIT_DEFAULT, RaftServerConfigKeys.getDefaultLog(), requireMinSizeInByte(SizeInBytes.ONE_MB));
  }

  static void setByteLimit(RaftProperties properties, SizeInBytes byteLimit) {
    setSizeInBytes(properties::set, BYTE_LIMIT_KEY, byteLimit, requireMin(1L));
  }

  String FOLLOWER_GAP_RATIO_MAX_KEY = PREFIX + ".follower.gap.ratio.max";
  // The valid range is [1, 0) and -1, -1 means disable this feature
  double FOLLOWER_GAP_RATIO_MAX_DEFAULT = -1d;

  static double followerGapRatioMax(RaftProperties properties) {
    return ConfUtils.getDouble(properties::getDouble, FOLLOWER_GAP_RATIO_MAX_KEY,
        FOLLOWER_GAP_RATIO_MAX_DEFAULT, RaftServerConfigKeys.getDefaultLog(), requireMax(1d));
  }

  static void setFollowerGapRatioMax(RaftProperties properties, float ratio) {
    setDouble(properties::setDouble, FOLLOWER_GAP_RATIO_MAX_KEY, ratio, requireMax(1d));
  }
}
