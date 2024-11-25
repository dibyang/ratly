
package net.xdob.ratly.server.protocol;

import java.io.IOException;

import net.xdob.ratly.proto.raft.AppendEntriesReplyProto;
import net.xdob.ratly.proto.raft.AppendEntriesRequestProto;
import net.xdob.ratly.proto.raft.InstallSnapshotReplyProto;
import net.xdob.ratly.proto.raft.InstallSnapshotRequestProto;
import net.xdob.ratly.proto.raft.RequestVoteReplyProto;
import net.xdob.ratly.proto.raft.RequestVoteRequestProto;
import net.xdob.ratly.proto.raft.StartLeaderElectionReplyProto;
import net.xdob.ratly.proto.raft.StartLeaderElectionRequestProto;

public interface RaftServerProtocol {
  enum Op {REQUEST_VOTE, APPEND_ENTRIES, INSTALL_SNAPSHOT}

  RequestVoteReplyProto requestVote(RequestVoteRequestProto request) throws IOException;

  AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto request) throws IOException;

  InstallSnapshotReplyProto installSnapshot(InstallSnapshotRequestProto request) throws IOException;

  StartLeaderElectionReplyProto startLeaderElection(StartLeaderElectionRequestProto request) throws IOException;
}
