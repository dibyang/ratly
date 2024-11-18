

package net.xdob.ratly.server.impl;

import net.xdob.ratly.proto.RaftProtos.CandidateInfoProto;
import net.xdob.ratly.proto.RaftProtos.FollowerInfoProto;
import net.xdob.ratly.proto.RaftProtos.LeaderInfoProto;
import net.xdob.ratly.proto.RaftProtos.RaftPeerRole;
import net.xdob.ratly.proto.RaftProtos.RoleInfoProto;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.leader.LogAppender;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static net.xdob.ratly.server.impl.ServerProtoUtils.toServerRpcProto;

/**
 * Maintain the Role of a Raft Peer.
 */
class RoleInfo {
  public static final Logger LOG = LoggerFactory.getLogger(RoleInfo.class);

  private final RaftPeerId id;
  private final AtomicReference<RaftPeerRole> role = new AtomicReference<>();
  /** Used when the peer is leader */
  private final AtomicReference<LeaderStateImpl> leaderState = new AtomicReference<>();
  /** Used when the peer is follower, to monitor election timeout */
  private final AtomicReference<FollowerState> followerState = new AtomicReference<>();
  /** Used when the peer is candidate, to request votes from other peers */
  private final AtomicReference<LeaderElection> leaderElection = new AtomicReference<>();
  private final AtomicBoolean pauseLeaderElection = new AtomicBoolean(false);

  private final AtomicReference<Timestamp> transitionTime;

  RoleInfo(RaftPeerId id) {
    this.id = id;
    this.transitionTime = new AtomicReference<>(Timestamp.currentTime());
  }

  void transitionRole(RaftPeerRole newRole) {
    this.role.set(newRole);
    this.transitionTime.set(Timestamp.currentTime());
  }

  long getRoleElapsedTimeMs() {
    return transitionTime.get().elapsedTimeMs();
  }

  RaftPeerRole getCurrentRole() {
    return role.get();
  }

  boolean isLeaderReady() {
    return getLeaderState().map(LeaderStateImpl::isReady).orElse(false);
  }

  Optional<LeaderStateImpl> getLeaderState() {
    return Optional.ofNullable(leaderState.get());
  }

  LeaderStateImpl getLeaderStateNonNull() {
    return Objects.requireNonNull(leaderState.get(), "leaderState is null");
  }

  LeaderStateImpl updateLeaderState(RaftServerImpl server) {
    return updateAndGet(leaderState, new LeaderStateImpl(server));
  }

  CompletableFuture<Void> shutdownLeaderState(boolean allowNull) {
    final LeaderStateImpl leader = leaderState.getAndSet(null);
    if (leader == null) {
      if (!allowNull) {
        return JavaUtils.completeExceptionally(new NullPointerException("leaderState == null"));
      }
      return CompletableFuture.completedFuture(null);
    } else {
      LOG.info("{}: shutdown {}", id, leader);
      return leader.stop();
    }
  }

  Optional<FollowerState> getFollowerState() {
    return Optional.ofNullable(followerState.get());
  }

  void startFollowerState(RaftServerImpl server, Object reason) {
    updateAndGet(followerState, new FollowerState(server, reason)).start();
  }

  CompletableFuture<Void> shutdownFollowerState() {
    final FollowerState follower = followerState.getAndSet(null);
    if (follower == null) {
      return CompletableFuture.completedFuture(null);
    }
    LOG.info("{}: shutdown {}", id, follower);
    return follower.stopRunning();
  }

  void startLeaderElection(RaftServerImpl server, boolean force) {
    if (pauseLeaderElection.get()) {
      return;
    }
    updateAndGet(leaderElection, new LeaderElection(server, force)).start();
  }

  void setLeaderElectionPause(boolean pause) {
    pauseLeaderElection.set(pause);
  }

  CompletableFuture<Void> shutdownLeaderElection() {
    final LeaderElection election = leaderElection.getAndSet(null);
    if (election == null) {
      return CompletableFuture.completedFuture(null);
    }
    LOG.info("{}: shutdown {}", id, election);
    return election.shutdown();
  }

  private <T> T updateAndGet(AtomicReference<T> ref, T current) {
    final T updated = ref.updateAndGet(previous -> previous != null? previous: current);
    Preconditions.assertTrue(updated == current, "previous != null");
    LOG.info("{}: start {}", id, current);
    return updated;
  }

  RoleInfoProto buildRoleInfoProto(RaftServerImpl server) {
    final RaftPeerRole currentRole = getCurrentRole();
    final RoleInfoProto.Builder proto = RoleInfoProto.newBuilder()
        .setSelf(server.getPeer().getRaftPeerProto())
        .setRole(currentRole)
        .setRoleElapsedTimeMs(getRoleElapsedTimeMs());

    switch (currentRole) {
      case LEADER:
        getLeaderState().ifPresent(leader -> {
          final LeaderInfoProto.Builder b = LeaderInfoProto.newBuilder()
              .setTerm(leader.getCurrentTerm());
          leader.getLogAppenders()
              .map(LogAppender::getFollower)
              .map(f -> toServerRpcProto(f.getPeer(), f.getLastRpcResponseTime().elapsedTimeMs()))
              .forEach(b::addFollowerInfo);
          proto.setLeaderInfo(b);
        });
        return proto.build();

      case CANDIDATE:
        return proto.setCandidateInfo(CandidateInfoProto.newBuilder()
            .setLastLeaderElapsedTimeMs(server.getState().getLastLeaderElapsedTimeMs()))
            .build();

      case LISTENER:
      case FOLLOWER:
        // FollowerState can be null while adding a new peer as it is not a voting member yet
        final FollowerState follower = getFollowerState().orElse(null);
        final long rpcElapsed;
        final int outstandingOp;
        if (follower != null) {
          rpcElapsed = follower.getLastRpcTime().elapsedTimeMs();
          outstandingOp = follower.getOutstandingOp();
        } else {
          rpcElapsed = 0;
          outstandingOp = 0;
        }
        final RaftPeer leader = server.getRaftConf().getPeer(server.getState().getLeaderId());
        return proto.setFollowerInfo(FollowerInfoProto.newBuilder()
            .setLeaderInfo(toServerRpcProto(leader, rpcElapsed))
            .setOutstandingOp(outstandingOp))
            .build();

      default:
        throw new IllegalStateException("Unexpected role " + currentRole);
    }
  }

  @Override
  public String toString() {
    return String.format("%9s", role);
  }
}
