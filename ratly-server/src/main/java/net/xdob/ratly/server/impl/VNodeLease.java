package net.xdob.ratly.server.impl;

import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.server.config.RaftServerConfigKeys;
import net.xdob.ratly.util.Timestamp;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 虚节点租期是否到期检测
 */
class VNodeLease {

  private final AtomicBoolean enabled;
  private final long leaseTimeoutMs;
  private final AtomicReference<Timestamp> lease = new AtomicReference<>(Timestamp.currentTime());

  VNodeLease(RaftProperties properties) {
    this.enabled = new AtomicBoolean(true);
    this.leaseTimeoutMs = RaftServerConfigKeys.Rpc.timeoutMax(properties)
      .multiply(3)
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
   * 主动过期
   */
  void invalid(){
    lease.set(Timestamp.currentTime().addTimeMs(leaseTimeoutMs));
  }

  synchronized void extend() {
    if (isEnabled()) {
      // update the new lease
      final Timestamp newLease = Timestamp.currentTime().addTimeMs(this.leaseTimeoutMs);
      lease.set(newLease);
    }
  }

}
