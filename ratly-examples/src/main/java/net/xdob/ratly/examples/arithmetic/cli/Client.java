package net.xdob.ratly.examples.arithmetic.cli;

import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.examples.common.SubCommandBase;
import net.xdob.ratly.grpc.GrpcFactory;
import net.xdob.ratly.protocol.ClientId;
import net.xdob.ratly.protocol.RaftGroup;
import net.xdob.ratly.protocol.RaftGroupId;
import com.google.protobuf.ByteString;

import java.io.IOException;

/**
 * Client to connect arithmetic example cluster.
 */
public abstract class Client extends SubCommandBase {


  @Override
  public void run() throws Exception {
    RaftProperties raftProperties = new RaftProperties();

    final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(getRaftGroupId())),
            getPeers());

    RaftClient.Builder builder =
        RaftClient.newBuilder().setProperties(raftProperties);
    builder.setRaftGroup(raftGroup);
    builder.setClientRpc(new GrpcFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), raftProperties));
    RaftClient client = builder.build();

    operation(client);


  }

  protected abstract void operation(RaftClient client) throws IOException;
}
