package net.xdob.ratly.examples.jdbc;

import com.beust.jcommander.Parameter;
import com.google.protobuf.ByteString;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.examples.common.SubCommandBase;
import net.xdob.ratly.grpc.GrpcConfigKeys;
import net.xdob.ratly.jdbc.JdbcStateMachine;
import net.xdob.ratly.protocol.RaftGroup;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.config.RaftServerConfigKeys;
import net.xdob.ratly.server.storage.StartupOption;
import net.xdob.ratly.util.LifeCycle;
import net.xdob.ratly.util.NetUtils;

import java.io.File;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class Server extends SubCommandBase {
  @Parameter(names = {"--id",
      "-i"}, description = "Raft id of this server", required = true)
  private String id;

  @Parameter(names = {"--storage",
      "-s"}, description = "Storage dir", required = true)
  private File storageDir;

  @Override
  public void run() throws Exception {
    RaftPeerId peerId = RaftPeerId.valueOf(id);
    RaftProperties properties = new RaftProperties();

    final int port = NetUtils.createSocketAddr(getPeer(peerId).getAddress()).getPort();
    GrpcConfigKeys.Server.setPort(properties, port);

    Optional.ofNullable(getPeer(peerId).getClientAddress()).ifPresent(address ->
        GrpcConfigKeys.Client.setPort(properties, NetUtils.createSocketAddr(address).getPort()));
    Optional.ofNullable(getPeer(peerId).getAdminAddress()).ifPresent(address ->
        GrpcConfigKeys.Admin.setPort(properties, NetUtils.createSocketAddr(address).getPort()));

    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));
    JdbcStateMachine stateMachine = new JdbcStateMachine("aio");

    final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(getRaftGroupId())),
        getPeers());
    RaftServer raftServer = RaftServer.newBuilder()
        .setServerId(RaftPeerId.valueOf(id))
        .setOption(StartupOption.RECOVER)
        .setStateMachine(stateMachine).setProperties(properties)
        .setGroup(raftGroup)
        .build();
    raftServer.start();

    for(; raftServer.getLifeCycleState() != LifeCycle.State.CLOSED;) {
      TimeUnit.SECONDS.sleep(1);
    }
  }
}
