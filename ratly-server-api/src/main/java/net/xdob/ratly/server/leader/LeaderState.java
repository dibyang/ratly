
package net.xdob.ratly.server.leader;

import net.xdob.ratly.proto.raft.AppendEntriesReplyProto;
import net.xdob.ratly.proto.raft.AppendEntriesRequestProto;
import net.xdob.ratly.proto.raft.LogEntryProto;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.util.JavaUtils;

import java.util.List;

/**
 * States for leader only.
 */
public interface LeaderState {
  /** The reasons that this leader steps down and becomes a follower. */
  enum StepDownReason {
    HIGHER_TERM, HIGHER_PRIORITY, LOST_MAJORITY_HEARTBEATS, STATE_MACHINE_EXCEPTION, JVM_PAUSE, FORCE;

    private final String longName = JavaUtils.getClassSimpleName(getClass()) + ":" + name();

    @Override
    public String toString() {
      return longName;
    }
  }

  /** Restart the given {@link LogAppender}. */
  void restart(LogAppender appender);

  /** @return a new {@link AppendEntriesRequestProto} object. */
  AppendEntriesRequestProto newAppendEntriesRequestProto(FollowerInfo follower,
      List<LogEntryProto> entries, TermIndex previous, long callId);

  /** Check if the follower is healthy. */
  void checkHealth(FollowerInfo follower);

  /** Handle the event that the follower has replied a term. */
  boolean onFollowerTerm(FollowerInfo follower, long followerTerm);

  /** Handle the event that the follower has replied a commit index. */
  void onFollowerCommitIndex(FollowerInfo follower, long commitIndex);

  /** Handle the event that the follower has replied a success append entries. */
  void onFollowerSuccessAppendEntries(FollowerInfo follower);

  /** Check if a follower is bootstrapping. */
  boolean isFollowerBootstrapping(FollowerInfo follower);

  /** Received an {@link AppendEntriesReplyProto} */
  void onAppendEntriesReply(LogAppender appender, AppendEntriesReplyProto reply);

}
