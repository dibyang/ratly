package net.xdob.ratly.protocol;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** Asynchronous version of {@link AdminProtocol}. */
public interface AdminAsynchronousProtocol {
  CompletableFuture<GroupListReply> getGroupListAsync(GroupListRequest request);

  CompletableFuture<GroupInfoReply> getGroupInfoAsync(GroupInfoRequest request);

  CompletableFuture<RaftClientReply> groupManagementAsync(GroupManagementRequest request);

  CompletableFuture<RaftClientReply> snapshotManagementAsync(SnapshotManagementRequest request);

  CompletableFuture<RaftClientReply> leaderElectionManagementAsync(LeaderElectionManagementRequest request);

  CompletableFuture<RaftClientReply> setConfigurationAsync(
      SetConfigurationRequest request) throws IOException;

  CompletableFuture<RaftClientReply> transferLeadershipAsync(
      TransferLeadershipRequest request) throws IOException;
}