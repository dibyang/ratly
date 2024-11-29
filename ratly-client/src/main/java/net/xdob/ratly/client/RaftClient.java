
package net.xdob.ratly.client;

import net.xdob.ratly.RaftConfigKeys;
import net.xdob.ratly.protocol.*;
import net.xdob.ratly.client.api.AdminApi;
import net.xdob.ratly.client.api.AsyncApi;
import net.xdob.ratly.client.api.BlockingApi;
import net.xdob.ratly.client.api.DataStreamApi;
import net.xdob.ratly.client.api.GroupManagementApi;
import net.xdob.ratly.client.api.LeaderElectionManagementApi;
import net.xdob.ratly.client.api.MessageStreamApi;
import net.xdob.ratly.client.api.SnapshotManagementApi;
import net.xdob.ratly.client.impl.ClientImplUtils;
import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.retry.RetryPolicies;
import net.xdob.ratly.retry.RetryPolicy;
import net.xdob.ratly.rpc.RpcType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collection;
import java.util.Objects;

/** A client who sends requests to a raft service. */
public interface RaftClient extends Closeable {
  Logger LOG = LoggerFactory.getLogger(RaftClient.class);

  /** @return the id of this client. */
  ClientId getId();

  /** @return the group id of this client. */
  RaftGroupId getGroupId();

  /** @return the cluster leaderId recorded by this client. */
  RaftPeerId getLeaderId();

  /** @return the {@link RaftClientRpc}. */
  RaftClientRpc getClientRpc();

  /** @return the {@link AdminApi}. */
  AdminApi admin();

  /** Get the {@link GroupManagementApi} for the given server. */
  GroupManagementApi getGroupManagementApi(RaftPeerId server);

  /** Get the {@link SnapshotManagementApi} for the given server. */
  SnapshotManagementApi getSnapshotManagementApi();

  /** Get the {@link SnapshotManagementApi} for the given server. */
  SnapshotManagementApi getSnapshotManagementApi(RaftPeerId server);

  /** Get the {@link LeaderElectionManagementApi} for the given server. */
  LeaderElectionManagementApi getLeaderElectionManagementApi(RaftPeerId server);

  /** @return the {@link BlockingApi}. */
  BlockingApi io();

  /** Get the {@link AsyncApi}. */
  AsyncApi async();

  /** @return the {@link MessageStreamApi}. */
  MessageStreamApi getMessageStreamApi();

  /** @return the {@link DataStreamApi}. */
  DataStreamApi getDataStreamApi();

  boolean isClosed();

  /** @return a {@link Builder}. */
  static Builder newBuilder() {
    return new Builder();
  }

  /** To build {@link RaftClient} objects. */
  final class Builder {
    private ClientId clientId;
    private RaftClientRpc clientRpc;
    private RaftGroup group;
    private RaftPeerId leaderId;
    private RaftPeer primaryDataStreamServer;
    private RaftProperties properties;
    private Parameters parameters;
    private RetryPolicy retryPolicy = RetryPolicies.retryForeverNoSleep();

    private Builder() {}

    /** @return a {@link RaftClient} object. */
    public RaftClient build() {
      if (clientId == null) {
        clientId = ClientId.randomId();
      }
      if (properties != null) {
        if (clientRpc == null) {
          final RpcType rpcType = RaftConfigKeys.Rpc.type(properties, LOG::debug);
          final ClientFactory factory = ClientFactory.cast(rpcType.newFactory(parameters));
          clientRpc = factory.newRaftClientRpc(clientId, properties);
        }
      }
      Objects.requireNonNull(group, "The 'group' field is not initialized.");
      if (primaryDataStreamServer == null) {
        final Collection<RaftPeer> peers = group.getPeers();
        if (!peers.isEmpty()) {
          primaryDataStreamServer = peers.iterator().next();
        }
      }
      return ClientImplUtils.newRaftClient(clientId, group, leaderId, primaryDataStreamServer,
          Objects.requireNonNull(clientRpc, "The 'clientRpc' field is not initialized."), retryPolicy,
          properties, parameters);
    }

    /** Set {@link RaftClient} ID. */
    public Builder setClientId(ClientId clientId) {
      this.clientId = clientId;
      return this;
    }

    /** Set servers. */
    public Builder setRaftGroup(RaftGroup grp) {
      this.group = grp;
      return this;
    }

    /** Set leader ID. */
    public Builder setLeaderId(RaftPeerId leaderId) {
      this.leaderId = leaderId;
      return this;
    }

    /** Set primary server of DataStream. */
    public Builder setPrimaryDataStreamServer(RaftPeer primaryDataStreamServer) {
      this.primaryDataStreamServer = primaryDataStreamServer;
      return this;
    }

    /** Set {@link RaftClientRpc}. */
    public Builder setClientRpc(RaftClientRpc clientRpc) {
      this.clientRpc = clientRpc;
      return this;
    }

    /** Set {@link RaftProperties}. */
    public Builder setProperties(RaftProperties properties) {
      this.properties = properties;
      return this;
    }

    /** Set {@link Parameters}. */
    public Builder setParameters(Parameters parameters) {
      this.parameters = parameters;
      return this;
    }

    /** Set {@link RetryPolicy}. */
    public Builder setRetryPolicy(RetryPolicy retryPolicy) {
      this.retryPolicy = retryPolicy;
      return this;
    }
  }
}
