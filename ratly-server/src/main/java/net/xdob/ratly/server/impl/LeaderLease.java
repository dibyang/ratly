package net.xdob.ratly.server.impl;

import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.config.Read;
import net.xdob.ratly.server.config.Rpc;
import net.xdob.ratly.server.leader.FollowerInfo;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.Timestamp;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class LeaderLease {

  private final AtomicBoolean enabled;
  private final long leaseTimeoutMs;
  private final AtomicReference<Timestamp> lease = new AtomicReference<>(Timestamp.currentTime());

  LeaderLease(RaftProperties properties) {
    this.enabled = new AtomicBoolean(Read.leaderLeaseEnabled(properties));
    final double leaseRatio = Read.leaderLeaseTimeoutRatio(properties);
    Preconditions.assertTrue(leaseRatio > 0.0 && leaseRatio <= 1.0,
        "leader ratio should sit in (0,1], now is " + leaseRatio);
    this.leaseTimeoutMs = Rpc.timeoutMin(properties)
        .multiply(leaseRatio)
        .toIntExact(TimeUnit.MILLISECONDS);
  }

  boolean getAndSetEnabled(boolean newValue) {
    return enabled.getAndSet(newValue);
  }

  boolean isEnabled() {
    return enabled.get();
  }

  boolean isValid() {
    return isEnabled() && lease.get().elapsedTimeMs() < leaseTimeoutMs;
  }

  /**
   * try extending the lease based on group heartbeats
   * @param old nullable
   */
  void extend(List<FollowerInfo> current, List<FollowerInfo> old, Predicate<List<RaftPeerId>> hasMajority) {
    final List<RaftPeerId> activePeers =
        // check the latest heartbeats of all peers (including those in transitional)
        Stream.concat(current.stream(), Optional.ofNullable(old).map(List::stream).orElse(Stream.empty()))
            .filter(f -> f.getLastRespondedAppendEntriesSendTime().elapsedTimeMs() < leaseTimeoutMs)
            .map(FollowerInfo::getId)
            .collect(Collectors.toList());

    if (!hasMajority.test(activePeers)) {
      return;
    }

    // update the new lease
    final Timestamp newLease =
        Timestamp.earliest(getMaxTimestampWithMajorityAck(current), getMaxTimestampWithMajorityAck(old));
    lease.set(newLease);
  }

  /**
   * return maximum timestamp at when the majority of followers are known to be active
   * return {@link Timestamp#currentTime()} if peers are empty
   */
  private Timestamp getMaxTimestampWithMajorityAck(List<FollowerInfo> followers) {
    if (followers == null || followers.isEmpty()) {
      return Timestamp.currentTime();
    }

    final long mid = followers.size() / 2;
    return followers.stream()
        .map(FollowerInfo::getLastRespondedAppendEntriesSendTime)
        .sorted()
        .limit(mid+1)
        .skip(mid)
        .iterator()
        .next();
  }
}
