package net.xdob.ratly.server.impl;

import net.xdob.ratly.proto.raft.*;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.DivisionInfo;
import net.xdob.ratly.server.RaftConfiguration;
import net.xdob.ratly.server.impl.LeaderElection.Phase;
import net.xdob.ratly.server.protocol.TermIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class VoteContext {
  static final Logger LOG = LoggerFactory.getLogger(VoteContext.class);

  private final RaftServerImpl impl;
  private final RaftConfiguration conf;
  private final Phase phase;
  private final RaftPeerId candidateId;

  VoteContext(RaftServerImpl impl, Phase phase, RaftPeerId candidateId) {
    this.impl = impl;
    this.conf = impl.getRaftConf();
    this.phase = phase;
    this.candidateId = candidateId;
  }

  private boolean reject(String reason) {
    return log(false, reason);
  }

  private boolean log(boolean accept, String reason) {
    LOG.info("{}-{}: {} {} from {}: {}",
        impl.getMemberId(), impl.getInfo().getCurrentRole(), accept? "accept": "reject", phase, candidateId, reason);
    return accept;
  }

  /** Check if the candidate is in the current conf. */
  private RaftPeer checkConf() {
    if (!conf.containsInConf(candidateId)) {
      reject(candidateId + " is not in current conf " + conf.getCurrentPeers());
      return null;
    }
    return conf.getPeer(candidateId);
  }

  enum CheckTermResult {
    FAILED, CHECK_LEADER, SKIP_CHECK_LEADER
  }

  /** Check the candidate term. */
  private CheckTermResult checkTerm(long candidateTerm) {
    if (phase == Phase.PRE_VOTE) {
      return CheckTermResult.CHECK_LEADER;
    }
    // check term
    final ServerState state = impl.getState();
    final long currentTerm = state.getCurrentTerm();
    if (currentTerm > candidateTerm) {
      reject("current term " + currentTerm + " > candidate's term " + candidateTerm);
      return CheckTermResult.FAILED;
    } else if (currentTerm == candidateTerm) {
      // check if this server has already voted
      final RaftPeerId votedFor = state.getVotedFor();
      if (votedFor != null && !votedFor.equals(candidateId)) {
        reject("already has voted for " + votedFor + " at current term " + currentTerm);
        return CheckTermResult.FAILED;
      }
      return CheckTermResult.CHECK_LEADER;
    } else {
      return CheckTermResult.SKIP_CHECK_LEADER; //currentTerm < candidateTerm
    }
  }

  /** Check if there is already a leader. */
  private boolean checkLeader() {
    // check if this server is the leader
    final DivisionInfo info = impl.getInfo();
    if (info.isLeader()) {
      if (impl.getRole().getLeaderState().map(LeaderStateImpl::checkLeadership).orElse(false)) {
        return reject("this server is the leader and still has leadership");
      }
    }

    // check if this server is a follower and has a valid leader
    if (info.isFollower()) {
      final RaftPeerId leader = impl.getState().getLeaderId();
      if (leader != null
          && !leader.equals(candidateId)
          && impl.getRole().getFollowerState().map(FollowerState::isCurrentLeaderValid).orElse(false)) {
        return reject("this server is a follower and still has a valid leader " + leader
            + " different than the candidate " + candidateId);
      }
    }
    return true;
  }

  RaftPeer recognizeCandidate(long candidateTerm) {
    final RaftPeer candidate = checkConf();
    if (candidate == null) {
      return null;
    }
    final CheckTermResult r = checkTerm(candidateTerm);
    if (r == CheckTermResult.FAILED) {
      return null;
    }
    if (r == CheckTermResult.CHECK_LEADER && !checkLeader()) {
      return null;
    }
    return candidate;
  }

  /**
   * 服务器在以下情况下应为候选人投票：
   *   1.本地日志落后于候选人的日志。
   *   2.本地日志与候选人日志相同，候选节点是虚拟节点，本地节点不是虚拟节点时拒绝投票；
   *   3.本地日志与候选人日志相同，候选节点不是虚拟或候选节点和本地节点都是虚拟节点，且优先级不高于候选人
   */
  boolean decideVote(RaftPeer candidate, TermIndex candidateLastEntry) {
    if (impl.getRole().getCurrentRole() == RaftPeerRole.LISTENER) {
      return reject("this server is a listener, who is a non-voting member");
    }
    if (candidate == null) {
      return false;
    }
    // Check last log entry
    final TermIndex lastEntry = impl.getState().getLastEntry();
    final int compare = ServerState.compareLog(lastEntry, candidateLastEntry);
    if (compare < 0) {
      return log(true, "our last entry " + lastEntry + " < candidate's last entry " + candidateLastEntry);
    } else if (compare > 0) {
      return reject("our last entry " + lastEntry + " > candidate's last entry " + candidateLastEntry);
    }

    // Check priority
    final RaftPeer peer = conf.getPeer(impl.getId());
    if (peer == null) {
      return reject("our server " + impl.getId() + " is not in the conf " + conf);
    }
    //索引数据一致，候选节点是虚拟节点，本地节点不是虚拟节点时拒绝投票
//    if(candidate.isVirtual()&&!peer.isVirtual()){
//      return reject("candidate's virtual node but our node is not virtual node");
//    }

    //获取本地优先级
    final int priority = peer.getPriority();
    if (priority <= candidate.getPriority()) {
      return log(true, "our priority " + priority + " <= candidate's priority " + candidate.getPriority());
    } else {
      return reject("our priority " + priority + " > candidate's priority " + candidate.getPriority());
    }
  }
}
