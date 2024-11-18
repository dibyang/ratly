
package net.xdob.ratly.protocol;

import java.io.IOException;

/** For server administration. */
public interface AdminProtocol {
  GroupListReply getGroupList(GroupListRequest request) throws IOException;

  GroupInfoReply getGroupInfo(GroupInfoRequest request) throws IOException;

  RaftClientReply groupManagement(GroupManagementRequest request) throws IOException;

  RaftClientReply snapshotManagement(SnapshotManagementRequest request) throws IOException;

  default RaftClientReply leaderElectionManagement(LeaderElectionManagementRequest request) throws IOException {
    throw new UnsupportedOperationException(getClass() + " does not support this method yet.");
  }

  RaftClientReply setConfiguration(SetConfigurationRequest request) throws IOException;

  RaftClientReply transferLeadership(TransferLeadershipRequest request) throws IOException;
}