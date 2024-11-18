
package net.xdob.ratly.server.protocol;

import java.io.IOException;

import net.xdob.ratly.proto.RaftProtos.AppendEntriesReplyProto;
import net.xdob.ratly.proto.RaftProtos.AppendEntriesRequestProto;
import net.xdob.ratly.proto.RaftProtos.InstallSnapshotReplyProto;
import net.xdob.ratly.proto.RaftProtos.InstallSnapshotRequestProto;
import net.xdob.ratly.proto.RaftProtos.RequestVoteReplyProto;
import net.xdob.ratly.proto.RaftProtos.RequestVoteRequestProto;
import net.xdob.ratly.proto.RaftProtos.StartLeaderElectionReplyProto;
import net.xdob.ratly.proto.RaftProtos.StartLeaderElectionRequestProto;

public interface RaftServerProtocol {
  enum Op {REQUEST_VOTE, APPEND_ENTRIES, INSTALL_SNAPSHOT}

  RequestVoteReplyProto requestVote(RequestVoteRequestProto request) throws IOException;

  AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto request) throws IOException;

  InstallSnapshotReplyProto installSnapshot(InstallSnapshotRequestProto request) throws IOException;

  StartLeaderElectionReplyProto startLeaderElection(StartLeaderElectionRequestProto request) throws IOException;
}
