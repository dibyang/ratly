package net.xdob.ratly.jdbc;

import com.google.protobuf.ByteString;
import net.xdob.ratly.proto.raft.RaftPeerRole;
import net.xdob.ratly.protocol.RaftGroup;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.RaftServer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class JdbcServer {
  private final String group;
  private final List<DbPeer> dbPeers = new ArrayList<>();

  public JdbcServer(String group, DbPeer ... dbPeers) {
    this(Arrays.asList(dbPeers), group);
  }
  public JdbcServer(Collection<DbPeer> dbPeers, String group) {
    this.group = group;
    this.dbPeers.addAll(dbPeers);
  }

  public String getGroup() {
    return group;
  }

  public List<DbPeer> getDbPeers() {
    return dbPeers;
  }

  public void start() throws IOException {
    List<RaftPeer> peers = getDbPeers().stream().map(e -> RaftPeer.newBuilder().setId(e.getId())
        .setAddress(e.getAddress()).setStartupRole(RaftPeerRole.FOLLOWER)
        .build()).collect(Collectors.toList());
    final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(getGroup())), peers);
    List<DbPeer> localNodes = getDbPeers().stream().filter(DbPeer::isLocal).collect(Collectors.toList());
    for (DbPeer p : localNodes) {
      RaftServer raftServer = RaftServer.newBuilder()
          .setServerId(RaftPeerId.valueOf(p.getId()))
          //.setStateMachine(stateMachine).setProperties(properties)
          .setGroup(raftGroup)
          .build();
      raftServer.start();
    }
  }
}
