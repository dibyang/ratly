package net.xdob.ratly;

import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.datastream.SupportedDataStreamType;
import net.xdob.ratly.rpc.RpcType;
import net.xdob.ratly.rpc.SupportedRpcType;

import java.util.function.Consumer;

import static net.xdob.ratly.conf.ConfUtils.*;

/**
 * RaftConfig支持的工具类
 */
public interface RaftConfigKeys {
  String PREFIX = "raft";

  interface Rpc {
    String PREFIX = RaftConfigKeys.PREFIX + ".rpc";

    String TYPE_KEY = PREFIX + ".type";
    String TYPE_DEFAULT = SupportedRpcType.GRPC.name();

    static RpcType type(RaftProperties properties, Consumer<String> logger) {
      final String t = get(properties::get, TYPE_KEY, TYPE_DEFAULT, logger);
      return RpcType.valueOf(t);
    }

    static void setType(RaftProperties properties, RpcType type) {
      set(properties::set, TYPE_KEY, type.name());
    }
  }

  interface DataStream {
    String PREFIX = RaftConfigKeys.PREFIX + ".datastream";

    String TYPE_KEY = PREFIX + ".type";
    String TYPE_DEFAULT = SupportedDataStreamType.DISABLED.name();

    static SupportedDataStreamType type(RaftProperties properties, Consumer<String> logger) {
      final String t = get(properties::get, TYPE_KEY, TYPE_DEFAULT, logger);
      return SupportedDataStreamType.valueOfIgnoreCase(t);
    }

    static void setType(RaftProperties properties, SupportedDataStreamType type) {
      set(properties::set, TYPE_KEY, type.name());
    }

    String SKIP_SEND_FORWARD_KEY = PREFIX + ".skip.send-forward";
    boolean SKIP_SEND_FORWARD_DEFAULT = false;

    static boolean skipSendForward(RaftProperties properties, Consumer<String> logger) {
      return getBoolean(properties::getBoolean, SKIP_SEND_FORWARD_KEY, SKIP_SEND_FORWARD_DEFAULT, logger);
    }

    static void setSkipSendForward(RaftProperties properties, boolean skipSendForward) {
      setBoolean(properties::setBoolean, SKIP_SEND_FORWARD_KEY, skipSendForward);
    }
  }

  static void main(String[] args) {
    printAll(RaftConfigKeys.class);
  }
}
