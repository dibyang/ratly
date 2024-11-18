package net.xdob.ratly.netty.server;

import net.xdob.ratly.protocol.ClientInvocationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * Map: {@link ClientInvocationId} -> {@link STREAM}.
 *
 * @param <STREAM> the stream type.
 */
class StreamMap<STREAM> {
  public static final Logger LOG = LoggerFactory.getLogger(StreamMap.class);

  private final ConcurrentMap<ClientInvocationId, STREAM> map = new ConcurrentHashMap<>();

  STREAM computeIfAbsent(ClientInvocationId key, Function<ClientInvocationId, STREAM> function) {
    final STREAM info = map.computeIfAbsent(key, function);
    LOG.debug("computeIfAbsent({}) returns {}", key, info);
    return info;
  }

  STREAM get(ClientInvocationId key) {
    final STREAM info = map.get(key);
    LOG.debug("get({}) returns {}", key, info);
    return info;
  }

  STREAM remove(ClientInvocationId key) {
    final STREAM info = map.remove(key);
    LOG.debug("remove({}) returns {}", key, info);
    return info;
  }
}
