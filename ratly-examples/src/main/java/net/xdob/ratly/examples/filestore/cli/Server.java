
package net.xdob.ratly.examples.filestore.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import net.xdob.ratly.RaftConfigKeys;
import net.xdob.ratly.conf.ConfUtils;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.datastream.SupportedDataStreamType;
import net.xdob.ratly.examples.common.SubCommandBase;
import net.xdob.ratly.examples.filestore.FileStoreCommon;
import net.xdob.ratly.examples.filestore.FileStoreStateMachine;
import net.xdob.ratly.grpc.GrpcConfigKeys;
import net.xdob.ratly.metrics.impl.JvmMetrics;
import net.xdob.ratly.netty.NettyConfigKeys;
import net.xdob.ratly.protocol.RaftGroup;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.config.DataStream;
import net.xdob.ratly.server.config.RaftServerConfigKeys;
import net.xdob.ratly.server.config.Rpc;
import net.xdob.ratly.server.config.Write;
import net.xdob.ratly.server.storage.StartupOption;
import net.xdob.ratly.statemachine.StateMachine;
import com.google.protobuf.ByteString;
import net.xdob.ratly.util.LifeCycle;
import net.xdob.ratly.util.NetUtils;
import net.xdob.ratly.util.SizeInBytes;
import net.xdob.ratly.util.TimeDuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Class to start a ratly filestore example server.
 */
@Parameters(commandDescription = "Start an filestore server")
public class Server extends SubCommandBase {

  @Parameter(names = {"--id", "-i"}, description = "Raft id of this server", required = true)
  private String id;

  @Parameter(names = {"--storage", "-s"}, description = "Storage dir, eg. --storage dir1 --storage dir2",
      required = true)
  private List<File> storageDir = new ArrayList<>();

  @Parameter(names = {"--writeThreadNum"}, description = "Number of write thread")
  private int writeThreadNum = 20;

  @Parameter(names = {"--readThreadNum"}, description = "Number of read thread")
  private int readThreadNum = 20;

  @Parameter(names = {"--commitThreadNum"}, description = "Number of commit thread")
  private int commitThreadNum = 3;

  @Parameter(names = {"--deleteThreadNum"}, description = "Number of delete thread")
  private int deleteThreadNum = 3;

  @Override
  public void run() throws Exception {
    JvmMetrics.initJvmMetrics(TimeDuration.valueOf(10, TimeUnit.SECONDS));

    RaftPeerId peerId = RaftPeerId.valueOf(id);
    RaftProperties properties = new RaftProperties();

    // Avoid leader change affect the performance
    Rpc.setTimeoutMin(properties, TimeDuration.valueOf(2, TimeUnit.SECONDS));
    Rpc.setTimeoutMax(properties, TimeDuration.valueOf(3, TimeUnit.SECONDS));

    final int port = NetUtils.createSocketAddr(getPeer(peerId).getAddress()).getPort();
    GrpcConfigKeys.Server.setPort(properties, port);

    Optional.ofNullable(getPeer(peerId).getClientAddress()).ifPresent(address ->
        GrpcConfigKeys.Client.setPort(properties, NetUtils.createSocketAddr(address).getPort()));
    Optional.ofNullable(getPeer(peerId).getAdminAddress()).ifPresent(address ->
        GrpcConfigKeys.Admin.setPort(properties, NetUtils.createSocketAddr(address).getPort()));

    String dataStreamAddress = getPeer(peerId).getDataStreamAddress();
    if (dataStreamAddress != null) {
      final int dataStreamport = NetUtils.createSocketAddr(dataStreamAddress).getPort();
      NettyConfigKeys.DataStream.setPort(properties, dataStreamport);
      RaftConfigKeys.DataStream.setType(properties, SupportedDataStreamType.NETTY);
    }
    RaftServerConfigKeys.setStorageDir(properties, storageDir);
    Write.setElementLimit(properties, 40960);
    Write.setByteLimit(properties, SizeInBytes.valueOf("1000MB"));
    ConfUtils.setFiles(properties::setFiles, FileStoreCommon.STATEMACHINE_DIR_KEY, storageDir);
    DataStream.setAsyncRequestThreadPoolSize(properties, writeThreadNum);
    DataStream.setAsyncWriteThreadPoolSize(properties, writeThreadNum);
    ConfUtils.setInt(properties::setInt, FileStoreCommon.STATEMACHINE_WRITE_THREAD_NUM, writeThreadNum);
    ConfUtils.setInt(properties::setInt, FileStoreCommon.STATEMACHINE_READ_THREAD_NUM, readThreadNum);
    ConfUtils.setInt(properties::setInt, FileStoreCommon.STATEMACHINE_COMMIT_THREAD_NUM, commitThreadNum);
    ConfUtils.setInt(properties::setInt, FileStoreCommon.STATEMACHINE_DELETE_THREAD_NUM, deleteThreadNum);
    StateMachine stateMachine = new FileStoreStateMachine(properties);

    final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(getRaftGroupId())),
            getPeers());
    RaftServer raftServer = RaftServer.newBuilder()
        .setServerId(RaftPeerId.valueOf(id))
        .setStateMachine(stateMachine).setProperties(properties)
        .setGroup(raftGroup)
        .setOption(StartupOption.RECOVER)
        .build();

    raftServer.start();

    while (raftServer.getLifeCycleState() != LifeCycle.State.CLOSED) {
      TimeUnit.SECONDS.sleep(1);
    }
  }
}
