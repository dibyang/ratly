package net.xdob.ratly.server;

import net.xdob.ratly.proto.raft.RaftPeerRole;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * The configuration of the raft cluster.
 * <p>
 * The configuration is stable if there is no on-going peer change. Otherwise,
 * the configuration is transitional, i.e. in the middle of a peer change.
 * <p>
 * The objects of this class are immutable.
 */
public final class RaftConfigurationImpl implements RaftConfiguration {
  /** Create a {@link Builder}. */
  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private PeerConfiguration oldConf;
    private PeerConfiguration conf;
    private long logEntryIndex = RaftLog.INVALID_LOG_INDEX;

    private boolean forceStable = false;
    private boolean forceTransitional = false;

    private Builder() {}

    public Builder setConf(PeerConfiguration conf) {
      Objects.requireNonNull(conf, "PeerConfiguration == null");
      Preconditions.assertTrue(this.conf == null, "conf is already set.");
      this.conf = conf;
      return this;
    }

    public Builder setConf(Iterable<RaftPeer> peers) {
      return setConf(new PeerConfiguration(peers));
    }

    public Builder setConf(Iterable<RaftPeer> peers, Iterable<RaftPeer> listeners) {
      return setConf(new PeerConfiguration(peers, listeners));
    }

    public Builder setConf(RaftConfiguration transitionalConf) {
      Objects.requireNonNull(transitionalConf, "transitionalConf == null");
      Preconditions.assertTrue(transitionalConf.isTransitional());

      Preconditions.assertTrue(!forceTransitional);
      forceStable = true;
      return setConf(transitionalConf.getConf());
    }


    Builder setOldConf(PeerConfiguration oldConf) {
      Objects.requireNonNull(oldConf, "oldConf == null");
      Preconditions.assertTrue(this.oldConf == null, "oldConf is already set.");
      this.oldConf = oldConf;
      return this;
    }

    public Builder setOldConf(Iterable<RaftPeer> oldPeers, Iterable<RaftPeer> oldListeners) {
      return setOldConf(new PeerConfiguration(oldPeers, oldListeners));
    }

    public Builder setOldConf(RaftConfiguration stableConf) {
      Objects.requireNonNull(stableConf, "stableConf == null");
      Preconditions.assertTrue(stableConf.isStable());

      Preconditions.assertTrue(!forceStable);
      forceTransitional = true;
      return setOldConf(stableConf.getConf());
    }

    public Builder setLogEntryIndex(long logEntryIndex) {
      Preconditions.assertTrue(logEntryIndex != RaftLog.INVALID_LOG_INDEX);
      Preconditions.assertTrue(this.logEntryIndex == RaftLog.INVALID_LOG_INDEX, "logEntryIndex is already set.");
      this.logEntryIndex = logEntryIndex;
      return this;
    }

    public RaftConfigurationImpl build() {
      if (forceTransitional) {
        Preconditions.assertTrue(oldConf != null);
      }
      if (forceStable) {
        Preconditions.assertTrue(oldConf == null);
      }
      return new RaftConfigurationImpl(conf, oldConf, logEntryIndex);
    }
  }

  /** Non-null only if this configuration is transitional. */
  private final PeerConfiguration oldConf;
  /**
   * The current peer configuration while this configuration is stable;
   * or the new peer configuration while this configuration is transitional.
   */
  private final PeerConfiguration conf;

  /** The index of the corresponding log entry for this configuration. */
  private final long logEntryIndex;

  private RaftConfigurationImpl(PeerConfiguration conf, PeerConfiguration oldConf,
      long logEntryIndex) {
    this.conf = Objects.requireNonNull(conf, "PeerConfiguration == null");
    this.oldConf = oldConf;
    this.logEntryIndex = logEntryIndex;
  }

  /** Is this configuration transitional, i.e. in the middle of a peer change? */
  @Override
  public boolean isTransitional() {
    return oldConf != null;
  }

  /** Is this configuration stable, i.e. no on-going peer change? */
  @Override
  public boolean isStable() {
    return oldConf == null;
  }

  @SuppressWarnings({"squid:S6466"})
  @Override
  public boolean containsInConf(RaftPeerId peerId, RaftPeerRole... roles) {
    if (roles == null || roles.length == 0) {
      return conf.contains(peerId);
    } else if (roles.length == 1) {
      return conf.contains(peerId, roles[0]);
    } else {
      return conf.contains(peerId, EnumSet.of(roles[0], roles)) != null;
    }
  }

  @Override
  public PeerConfiguration getConf() {
    return conf;
  }

  @Override
  public PeerConfiguration getOldConf() {
    return oldConf;
  }

  @Override
  public boolean isHighestPriority(RaftPeerId peerId) {
    RaftPeer target = getPeer(peerId);
    if (target == null) {
      return false;
    }
    Collection<RaftPeer> peers = getCurrentPeers();
    for (RaftPeer peer : peers) {
      if (peer.getPriority() > target.getPriority() && !peer.equals(target)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean containsInOldConf(RaftPeerId peerId) {
    return oldConf != null && oldConf.contains(peerId);
  }

  /**
   * @return 如果给定的节点包含在 conf 中，并且如果旧 conf 存在，则节点也包含在旧 conf 中。返回true
   */
  @Override
  public boolean containsInBothConfs(RaftPeerId peerId) {
    return containsInConf(peerId) &&
        (oldConf == null || containsInOldConf(peerId));
  }

  @Override
  public RaftPeer getPeer(RaftPeerId id, RaftPeerRole... roles) {
    if (id == null) {
      return null;
    }
    final RaftPeer peer = conf.getPeer(id, roles);
    if (peer != null) {
      return peer;
    } else if (oldConf != null) {
      return oldConf.getPeer(id, roles);
    }
    return null;
  }

  @Override
  public List<RaftPeer> getAllPeers(RaftPeerRole role) {
    final List<RaftPeer> peers = new ArrayList<>(conf.getPeers(role));
    if (oldConf != null) {
      oldConf.getPeers(role).stream()
          .filter(p -> !peers.contains(p))
          .forEach(peers::add);
    }
    return peers;
  }

  /**
   * @return all the peers other than the given self id from the conf,
   *         and the old conf if it exists.
   */
  @Override
  public List<RaftPeer> getOtherPeers(RaftPeerId selfId) {
    List<RaftPeer> others = conf.getOtherPeers(selfId);
    if (oldConf != null) {
      oldConf.getOtherPeers(selfId).stream()
          .filter(p -> !others.contains(p))
          .forEach(others::add);
    }
    return others;
  }

  /**
   * @return true if the new peers number reaches half of new conf peers number or the group is
   * changing from single mode to HA mode.
   */
  @Override
  public boolean changeMajority(Collection<RaftPeer> newMembers) {
    Preconditions.assertNull(oldConf, "oldConf");
    final long newPeersCount = newMembers.stream().map(RaftPeer::getId).filter(id -> conf.getPeer(id) == null).count();

    if (conf.size() == 1 && newMembers.size() == 2 && newPeersCount == 1) {
      // Change from single peer to HA mode. This is a special case, skip majority verification.
      return false;
    }

    // If newPeersCount reaches majority number of new conf size, the cluster may end with infinity
    // election. See https://issues.apache.org/jira/browse/ratly-1912 for more details.
    final long oldPeersCount = newMembers.size() - newPeersCount;
    return newPeersCount >= oldPeersCount;
  }

  /** @return True if the selfId is in single mode. */
  @Override
  public boolean isSingleMode(RaftPeerId selfId) {
    if (isStable()) {
      return conf.size() == 1;
    } else {
      return oldConf.size() == 1 && oldConf.contains(selfId) && conf.size() == 2 && conf.contains(selfId);
    }
  }

  /**
   * 是否2节点模式
   */
  @Override
  public boolean isTwoNodeMode() {
    return conf.size() == 2;
  }

  /** @return true if the self id together with the others are in the majority. */
  @Override
  public boolean hasMajority(Collection<RaftPeerId> others, RaftPeerId selfId) {
    Preconditions.assertTrue(!others.contains(selfId));
    return conf.hasMajority(others, selfId) &&
        (oldConf == null || oldConf.hasMajority(others, selfId));
  }

  /** @return true if the self id together with the acknowledged followers reach majority. */
  @Override
  public boolean hasMajority(Predicate<RaftPeerId> followers, RaftPeerId selfId) {
    final boolean includeInCurrent = containsInConf(selfId);
    final boolean hasMajorityInNewConf = conf.hasMajority(followers, includeInCurrent);

    if (!isTransitional()) {
      return hasMajorityInNewConf;
    } else {
      final boolean includeInOldConf = containsInOldConf(selfId);
      final boolean hasMajorityInOldConf = oldConf.hasMajority(followers, includeInOldConf);
      return hasMajorityInOldConf && hasMajorityInNewConf;
    }
  }

  @Deprecated
  @Override
  public int getMajorityCount() {
    return conf.getMajorityCount();
  }

  /** @return true if the rejects are in the majority(maybe half is enough in some cases). */
  @Override
  public boolean majorityRejectVotes(Collection<RaftPeerId> rejects) {
    return conf.majorityRejectVotes(rejects) ||
            (oldConf != null && oldConf.majorityRejectVotes(rejects));
  }

  /** @return true if only one voting member (the leader) in the cluster */
  @Override
  public boolean isSingleton() {
    return getCurrentPeers().size() == 1 && getPreviousPeers().size() <= 1;
  }


  @Override
  public String toString() {
    return "conf: {index: " + logEntryIndex + ", cur=" + conf + ", old=" + oldConf + "}";
  }

  @Override
  public boolean hasNoChange(Collection<RaftPeer> newMembers, Collection<RaftPeer> newListeners) {
    if (!isStable() || conf.size() != newMembers.size()
        || conf.getPeers(RaftPeerRole.LISTENER).size() != newListeners.size()) {
      return false;
    }
    for (RaftPeer peer : newMembers) {
      final RaftPeer inConf = conf.getPeer(peer.getId());
      if (inConf == null) {
        return false;
      }
      if (inConf.getPriority() != peer.getPriority()) {
        return false;
      }
    }
    for (RaftPeer peer : newListeners) {
      final RaftPeer inConf = conf.getPeer(peer.getId(), RaftPeerRole.LISTENER);
      if (inConf == null) {
        return false;
      }
      if (inConf.getPriority() != peer.getPriority()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public long getLogEntryIndex() {
    return logEntryIndex;
  }

  /** @return the peers which are not contained in conf. */
  @Override
  public List<RaftPeer> filterNotContainedInConf(List<RaftPeer> peers) {
    return peers.stream()
        .filter(p -> !containsInConf(p.getId(), RaftPeerRole.FOLLOWER, RaftPeerRole.LISTENER))
        .collect(Collectors.toList());
  }

  @Override
  public List<RaftPeer> getPreviousPeers(RaftPeerRole role) {
    return oldConf != null ? oldConf.getPeers(role) : Collections.emptyList();
  }

  @Override
  public List<RaftPeer> getCurrentPeers(RaftPeerRole role) {
    return conf.getPeers(role);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    final RaftConfigurationImpl that = (RaftConfigurationImpl)obj;
    return this.logEntryIndex == that.logEntryIndex
        && Objects.equals(this.conf,  that.conf)
        && Objects.equals(this.oldConf,  that.oldConf);
  }

  @Override
  public int hashCode() {
    return Long.hashCode(logEntryIndex);
  }
}
