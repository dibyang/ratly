
package net.xdob.ratly.client.api;

import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.protocol.SetConfigurationRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * An API to support administration
 * such as setting raft configuration and transferring leadership.
 */
public interface AdminApi {
  RaftClientReply setConfiguration(SetConfigurationRequest.Arguments arguments)
      throws IOException;

  /** The same as setConfiguration(serversInNewConf, Collections.emptyList()). */
  default RaftClientReply setConfiguration(List<RaftPeer> serversInNewConf) throws IOException {
    return setConfiguration(serversInNewConf, Collections.emptyList());
  }

  /** The same as setConfiguration(Arrays.asList(serversInNewConf)). */
  default RaftClientReply setConfiguration(RaftPeer[] serversInNewConf) throws IOException {
    return setConfiguration(Arrays.asList(serversInNewConf), Collections.emptyList());
  }

  /** Set the configuration request to the raft service. */
  default RaftClientReply setConfiguration(List<RaftPeer> serversInNewConf, List<RaftPeer> listenersInNewConf)
      throws IOException {
    return setConfiguration(SetConfigurationRequest.Arguments
        .newBuilder()
        .setServersInNewConf(serversInNewConf)
        .setListenersInNewConf(listenersInNewConf)
        .build());
  }

  /** The same as setConfiguration(Arrays.asList(serversInNewConf), Arrays.asList(listenersInNewConf)). */
  default RaftClientReply setConfiguration(RaftPeer[] serversInNewConf, RaftPeer[] listenersInNewConf)
      throws IOException {
    return setConfiguration(SetConfigurationRequest.Arguments
        .newBuilder()
        .setListenersInNewConf(serversInNewConf)
        .setListenersInNewConf(listenersInNewConf)
        .build());
  }

  /** Transfer leadership to the given server.*/
  default RaftClientReply transferLeadership(RaftPeerId newLeader, long timeoutMs) throws IOException {
    return transferLeadership(newLeader, null, timeoutMs);
  }

  RaftClientReply transferLeadership(RaftPeerId newLeader, RaftPeerId leaderId, long timeoutMs) throws IOException;
}