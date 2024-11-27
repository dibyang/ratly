package net.xdob.ratly.server.config;

import net.xdob.ratly.conf.ConfUtils;
import net.xdob.ratly.conf.RaftProperties;

import static net.xdob.ratly.conf.ConfUtils.*;
import static net.xdob.ratly.conf.ConfUtils.getInt;

public interface ThreadPool {
  String PREFIX = RaftServerConfigKeys.PREFIX + ".threadpool";

  String PROXY_CACHED_KEY = PREFIX + ".proxy.cached";
  boolean PROXY_CACHED_DEFAULT = true;

  static boolean proxyCached(RaftProperties properties) {
    return getBoolean(properties::getBoolean, PROXY_CACHED_KEY, PROXY_CACHED_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setProxyCached(RaftProperties properties, boolean useCached) {
    setBoolean(properties::setBoolean, PROXY_CACHED_KEY, useCached);
  }

  String PROXY_SIZE_KEY = PREFIX + ".proxy.size";
  int PROXY_SIZE_DEFAULT = 0;

  static int proxySize(RaftProperties properties) {
    return ConfUtils.getInt(properties::getInt, PROXY_SIZE_KEY, PROXY_SIZE_DEFAULT, RaftServerConfigKeys.getDefaultLog(),
        requireMin(0), requireMax(65536));
  }

  static void setProxySize(RaftProperties properties, int size) {
    setInt(properties::setInt, PROXY_SIZE_KEY, size);
  }

  String SERVER_CACHED_KEY = PREFIX + ".server.cached";
  boolean SERVER_CACHED_DEFAULT = true;

  static boolean serverCached(RaftProperties properties) {
    return getBoolean(properties::getBoolean, SERVER_CACHED_KEY, SERVER_CACHED_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setServerCached(RaftProperties properties, boolean useCached) {
    setBoolean(properties::setBoolean, SERVER_CACHED_KEY, useCached);
  }

  String SERVER_SIZE_KEY = PREFIX + ".server.size";
  int SERVER_SIZE_DEFAULT = 0;

  static int serverSize(RaftProperties properties) {
    return ConfUtils.getInt(properties::getInt, SERVER_SIZE_KEY, SERVER_SIZE_DEFAULT, RaftServerConfigKeys.getDefaultLog(),
        requireMin(0), requireMax(65536));
  }

  static void setServerSize(RaftProperties properties, int size) {
    setInt(properties::setInt, SERVER_SIZE_KEY, size);
  }

  String CLIENT_CACHED_KEY = PREFIX + ".client.cached";
  boolean CLIENT_CACHED_DEFAULT = true;

  static boolean clientCached(RaftProperties properties) {
    return getBoolean(properties::getBoolean, CLIENT_CACHED_KEY, CLIENT_CACHED_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setClientCached(RaftProperties properties, boolean useCached) {
    setBoolean(properties::setBoolean, CLIENT_CACHED_KEY, useCached);
  }

  String CLIENT_SIZE_KEY = PREFIX + ".client.size";
  int CLIENT_SIZE_DEFAULT = 0;

  static int clientSize(RaftProperties properties) {
    return ConfUtils.getInt(properties::getInt, CLIENT_SIZE_KEY, CLIENT_SIZE_DEFAULT, RaftServerConfigKeys.getDefaultLog(),
        requireMin(0), requireMax(65536));
  }

  static void setClientSize(RaftProperties properties, int size) {
    setInt(properties::setInt, CLIENT_SIZE_KEY, size);
  }
}
