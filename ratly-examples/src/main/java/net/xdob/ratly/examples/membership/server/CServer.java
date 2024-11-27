
package net.xdob.ratly.examples.membership.server;

import net.xdob.ratly.RaftConfigKeys;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.examples.counter.server.CounterStateMachine;
import net.xdob.ratly.netty.NettyConfigKeys;
import net.xdob.ratly.protocol.RaftGroup;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.rpc.SupportedRpcType;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.config.RaftServerConfigKeys;
import net.xdob.ratly.server.storage.RaftStorage;
import com.google.common.base.MoreObjects;
import net.xdob.ratly.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

/**
 * A simple raft server using {@link CounterStateMachine}.
 */
public class CServer {
  public static final RaftGroupId GROUP_ID = RaftGroupId.randomId();
  public static final String LOCAL_ADDR = "0.0.0.0";

  private final RaftServer server;
  private final int port;
  private final File storageDir;

  public CServer(RaftGroup group, RaftPeerId serverId, int port) throws IOException {
    this.storageDir = new File("./" + serverId);
    this.port = port;

    final RaftProperties properties = new RaftProperties();
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));
    RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.NETTY);
    NettyConfigKeys.Server.setPort(properties, port);

    // create the counter state machine which holds the counter value.
    final CounterStateMachine counterStateMachine = new CounterStateMachine();

    // build the Raft server.
    this.server = RaftServer.newBuilder()
        .setGroup(group)
        .setProperties(properties)
        .setServerId(serverId)
        .setStateMachine(counterStateMachine)
        .setOption(RaftStorage.StartupOption.FORMAT)
        .build();
  }

  public void start() throws IOException {
    server.start();
  }

  public RaftPeer getPeer() {
    return server.getPeer();
  }

  public void close() throws IOException {
    server.close();
    FileUtils.deleteFully(storageDir);
  }

  @Override
  public String toString() {
    try {
      return MoreObjects.toStringHelper(this)
          .add("server", server.getPeer())
          .add("role", server.getDivision(GROUP_ID).getInfo().getCurrentRole())
          .toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public int getPort() {
    return port;
  }
}
