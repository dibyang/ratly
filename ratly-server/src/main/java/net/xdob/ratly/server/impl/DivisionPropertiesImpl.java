
package net.xdob.ratly.server.impl;

import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.server.DivisionProperties;
import net.xdob.ratly.server.RaftServerConfigKeys;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.TimeDuration;

class DivisionPropertiesImpl implements DivisionProperties {
  private final TimeDuration rpcTimeoutMin;
  private final TimeDuration rpcTimeoutMax;
  private final TimeDuration rpcSleepTime;
  private final TimeDuration rpcSlownessTimeout;

  DivisionPropertiesImpl(RaftProperties properties) {
    this.rpcTimeoutMin = RaftServerConfigKeys.Rpc.timeoutMin(properties);
    this.rpcTimeoutMax = RaftServerConfigKeys.Rpc.timeoutMax(properties);
    Preconditions.assertTrue(rpcTimeoutMax.compareTo(rpcTimeoutMin) >= 0,
        "rpcTimeoutMax = %s < rpcTimeoutMin = %s", rpcTimeoutMax, rpcTimeoutMin);

    this.rpcSleepTime = RaftServerConfigKeys.Rpc.sleepTime(properties);
    this.rpcSlownessTimeout = RaftServerConfigKeys.Rpc.slownessTimeout(properties);
  }

  @Override
  public TimeDuration minRpcTimeout() {
    return rpcTimeoutMin;
  }

  @Override
  public TimeDuration maxRpcTimeout() {
    return rpcTimeoutMax;
  }

  @Override
  public TimeDuration rpcSleepTime() {
    return rpcSleepTime;
  }

  @Override
  public TimeDuration rpcSlownessTimeout() {
    return rpcSlownessTimeout;
  }
}