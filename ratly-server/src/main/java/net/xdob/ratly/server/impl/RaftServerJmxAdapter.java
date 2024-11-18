
package net.xdob.ratly.server.impl;

import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.RaftServerMXBean;
import net.xdob.ratly.util.JmxRegister;

import javax.management.ObjectName;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** JMX for {@link RaftServerImpl}. */
class RaftServerJmxAdapter extends JmxRegister implements RaftServerMXBean {
  static boolean registerMBean(String id, String groupId, RaftServerMXBean mBean, JmxRegister jmx) {
    final String prefix = "Ratly:service=RaftServer,group=" + groupId + ",id=";
    final String registered = jmx.register(mBean, Arrays.asList(
        () -> prefix + id,
        () -> prefix + ObjectName.quote(id)));
    return registered != null;
  }

  private final RaftServerImpl server;

  RaftServerJmxAdapter(RaftServerImpl server) {
    this.server = server;
  }

  boolean registerMBean() {
    return registerMBean(getId(), getGroupId(), this, this);
  }

  @Override
  public String getId() {
    return server.getId().toString();
  }

  @Override
  public String getLeaderId() {
    RaftPeerId leaderId = server.getState().getLeaderId();
    if (leaderId != null) {
      return leaderId.toString();
    } else {
      return null;
    }
  }

  @Override
  public long getCurrentTerm() {
    return server.getState().getCurrentTerm();
  }

  @Override
  public String getGroupId() {
    return server.getMemberId().getGroupId().toString();
  }

  @Override
  public String getRole() {
    return server.getRole().toString();
  }

  @Override
  public List<String> getFollowers() {
    return server.getRole().getLeaderState()
        .map(LeaderStateImpl::getFollowers)
        .orElseGet(Stream::empty)
        .map(RaftPeer::toString)
        .collect(Collectors.toList());
  }

  @Override
  public List<String> getGroups() {
    return server.getRaftServer().getGroupIds().stream()
        .map(RaftGroupId::toString)
        .collect(Collectors.toList());
  }
}
