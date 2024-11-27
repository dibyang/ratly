package net.xdob.ratly.server.config;

import net.xdob.ratly.conf.ConfUtils;
import net.xdob.ratly.conf.RaftProperties;

import static net.xdob.ratly.conf.ConfUtils.*;
import static net.xdob.ratly.conf.ConfUtils.getInt;

public interface DataStream {
  String PREFIX = RaftServerConfigKeys.PREFIX + ".data-stream";

  String ASYNC_REQUEST_THREAD_POOL_CACHED_KEY = PREFIX + ".async.request.thread.pool.cached";
  boolean ASYNC_REQUEST_THREAD_POOL_CACHED_DEFAULT = false;

  static boolean asyncRequestThreadPoolCached(RaftProperties properties) {
    return getBoolean(properties::getBoolean, ASYNC_REQUEST_THREAD_POOL_CACHED_KEY,
        ASYNC_REQUEST_THREAD_POOL_CACHED_DEFAULT, RaftServerConfigKeys.getDefaultLog());
  }

  static void setAsyncRequestThreadPoolCached(RaftProperties properties, boolean useCached) {
    setBoolean(properties::setBoolean, ASYNC_REQUEST_THREAD_POOL_CACHED_KEY, useCached);
  }

  String ASYNC_REQUEST_THREAD_POOL_SIZE_KEY = PREFIX + ".async.request.thread.pool.size";
  int ASYNC_REQUEST_THREAD_POOL_SIZE_DEFAULT = 32;

  static int asyncRequestThreadPoolSize(RaftProperties properties) {
    return ConfUtils.getInt(properties::getInt, ASYNC_REQUEST_THREAD_POOL_SIZE_KEY,
        ASYNC_REQUEST_THREAD_POOL_SIZE_DEFAULT, RaftServerConfigKeys.getDefaultLog(),
        requireMin(0), requireMax(65536));
  }

  static void setAsyncRequestThreadPoolSize(RaftProperties properties, int size) {
    setInt(properties::setInt, ASYNC_REQUEST_THREAD_POOL_SIZE_KEY, size);
  }

  String ASYNC_WRITE_THREAD_POOL_SIZE_KEY = PREFIX + ".async.write.thread.pool.size";
  int ASYNC_WRITE_THREAD_POOL_SIZE_DEFAULT = 16;

  static int asyncWriteThreadPoolSize(RaftProperties properties) {
    return ConfUtils.getInt(properties::getInt, ASYNC_WRITE_THREAD_POOL_SIZE_KEY,
        ASYNC_WRITE_THREAD_POOL_SIZE_DEFAULT, RaftServerConfigKeys.getDefaultLog(),
        requireMin(0), requireMax(65536));
  }

  static void setAsyncWriteThreadPoolSize(RaftProperties properties, int size) {
    setInt(properties::setInt, ASYNC_WRITE_THREAD_POOL_SIZE_KEY, size);
  }

  String CLIENT_POOL_SIZE_KEY = PREFIX + ".client.pool.size";
  int CLIENT_POOL_SIZE_DEFAULT = 10;

  static int clientPoolSize(RaftProperties properties) {
    return ConfUtils.getInt(properties::getInt, CLIENT_POOL_SIZE_KEY,
        CLIENT_POOL_SIZE_DEFAULT, RaftServerConfigKeys.getDefaultLog(),
        requireMin(0), requireMax(65536));
  }

  static void setClientPoolSize(RaftProperties properties, int num) {
    setInt(properties::setInt, CLIENT_POOL_SIZE_KEY, num);
  }
}
