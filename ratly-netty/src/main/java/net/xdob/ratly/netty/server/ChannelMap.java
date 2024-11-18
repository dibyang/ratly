package net.xdob.ratly.netty.server;

import net.xdob.ratly.protocol.ClientInvocationId;
import io.netty.channel.ChannelId;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Map: {@link ChannelId} -> {@link ClientInvocationId}s. */
class ChannelMap {
  private final Map<ChannelId, Map<ClientInvocationId, ClientInvocationId>> map = new ConcurrentHashMap<>();

  void add(ChannelId channelId, ClientInvocationId clientInvocationId) {
    map.computeIfAbsent(channelId, (e) -> new ConcurrentHashMap<>())
        .put(clientInvocationId, clientInvocationId);
  }

  void remove(ChannelId channelId, ClientInvocationId clientInvocationId) {
    Optional.ofNullable(map.get(channelId))
        .ifPresent((ids) -> ids.remove(clientInvocationId));
  }

  Set<ClientInvocationId> remove(ChannelId channelId) {
    return Optional.ofNullable(map.remove(channelId))
        .map(Map::keySet)
        .orElse(Collections.emptySet());
  }
}
