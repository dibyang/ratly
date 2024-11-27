
package net.xdob.ratly.server;

import net.xdob.ratly.server.config.RaftServerConfigKeys;
import net.xdob.ratly.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 接口描述了 Raft 服务器分区（Division）的一些配置属性，
 * 主要集中在 RPC（远程过程调用）的超时设置等方面。
 *
 * @see RaftServerConfigKeys
 */
public interface DivisionProperties {
  Logger LOG = LoggerFactory.getLogger(DivisionProperties.class);

  /** @return the minimum rpc timeout. */
  TimeDuration minRpcTimeout();

  /** @return the minimum rpc timeout in milliseconds. */
  default int minRpcTimeoutMs() {
    return minRpcTimeout().toIntExact(TimeUnit.MILLISECONDS);
  }

  /** @return the maximum rpc timeout. */
  TimeDuration maxRpcTimeout();

  /** @return the maximum rpc timeout in milliseconds. */
  default int maxRpcTimeoutMs() {
    return maxRpcTimeout().toIntExact(TimeUnit.MILLISECONDS);
  }

  /** @return the rpc sleep time period. */
  TimeDuration rpcSleepTime();

  /** @return the rpc slowness timeout. */
  TimeDuration rpcSlownessTimeout();
}
