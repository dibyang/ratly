
package net.xdob.ratly.examples.membership.server;

import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.examples.counter.CounterCommand;
import net.xdob.ratly.netty.NettyFactory;
import net.xdob.ratly.protocol.ClientId;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftGroup;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static net.xdob.ratly.examples.membership.server.CServer.GROUP_ID;
import static net.xdob.ratly.examples.membership.server.CServer.LOCAL_ADDR;

/**
 * An in process raft cluster. Running all servers in a single process.
 */
public class RaftCluster {
  private Map<Integer, CServer> members = new HashMap<>();

  /**
   * Start cluster.
   *
   * @param initPorts the ports of the initial peers.
   */
  public void init(Collection<Integer> initPorts) throws IOException {
    RaftGroup group = initGroup(initPorts);
    for (int port : initPorts) {
      CServer server = new CServer(group, peerId(port), port);
      server.start();
      members.put(port, server);
    }
  }

  /**
   * Update membership to C_new.
   *
   * @param newPorts the ports of the C_new peers.
   */
  public void update(Collection<Integer> newPorts) throws IOException {
    Preconditions.assertTrue(members.size() > 0, "Cluster is empty.");

    Collection<CServer> oldPeers = members.values();
    List<CServer> newPeers = new ArrayList<>();
    List<CServer> peerToStart = new ArrayList<>();
    List<CServer> peerToStop = new ArrayList<>();

    for (Integer port : newPorts) {
      CServer server = members.get(port);
      if (server == null) {
        // New peer always start with an empty group.
        RaftGroup group = RaftGroup.valueOf(GROUP_ID);
        server = new CServer(group, peerId(port), port);
        peerToStart.add(server);
      }
      newPeers.add(server);
    }

    for (CServer peer : oldPeers) {
      if (!newPeers.contains(peer)) {
        peerToStop.add(peer);
      }
    }

    // Step 1: start new peers.
    System.out.println("Update membership ...... Step 1: start new peers.");
    System.out.println(peersInfo(peerToStart, "Peers_to_start"));
    for (CServer server : peerToStart) {
      server.start();
    }

    // Step 2: update membership.
    System.out.println("Update membership ...... Step 2: update membership from C_old to C_new.");
    System.out.println(peersInfo(oldPeers, "C_old"));
    System.out.println(peersInfo(newPeers, "C_new"));
    if (members.size() > 0) {
      try (RaftClient client = createClient()) {
        RaftClientReply reply = client.admin().setConfiguration(newPeers.stream()
            .map(CServer::getPeer).collect(Collectors.toList()));
        if (!reply.isSuccess()) {
          throw reply.getException();
        }
      }
    }

    // Step 3: stop outdated peers.
    System.out.println("Update membership ...... Step 3: stop outdated peers.");
    System.out.println(peersInfo(peerToStop, "Peers_to_stop"));
    for (CServer server : peerToStop) {
      server.close();
      members.remove(server.getPort());
    }

    // Add new peers to members.
    for (CServer server : peerToStart) {
      members.put(server.getPort(), server);
    }
  }

  public void show() {
    Collection<CServer> peers = members.values();
    System.out.println(peersInfo(peers, "Cluster members"));
  }

  public void counterIncrement() throws IOException {
    RaftClient client = createClient();
    try {
      RaftClientReply reply = client.io().send(CounterCommand.INCREMENT.getMessage());
      if (!reply.isSuccess()) {
        throw reply.getException();
      }
    } finally {
      client.close();
    }
  }

  public void queryCounter() throws IOException {
    RaftClient client = createClient();
    try {
      RaftClientReply reply = client.io().sendReadOnly(CounterCommand.GET.getMessage());
      String count = reply.getMessage().getContent().toStringUtf8();
      System.out.println("Current counter value: " + count);
    } finally {
      client.close();
    }
  }

  /**
   * Configure the raft group with initial peers.
   */
  private RaftGroup initGroup(Collection<Integer> ports) {
    List<RaftPeer> peers = new ArrayList<>();
    for (int port : ports) {
      peers.add(RaftPeer.newBuilder()
          .setId(peerId(port))
          .setAddress(LOCAL_ADDR + ":" + port)
          .build());
    }
    members.values().stream().map(CServer::getPeer).forEach(peers::add);
    return RaftGroup.valueOf(GROUP_ID, peers);
  }

  public Collection<Integer> ports() {
    return members.keySet();
  }

  public void close() throws IOException {
    for (CServer server : members.values()) {
      server.close();
    }
  }

  private RaftClient createClient() {
    RaftProperties properties = new RaftProperties();
    RaftClient.Builder builder = RaftClient.newBuilder().setProperties(properties);

    builder.setRaftGroup(RaftGroup.valueOf(GROUP_ID,
        members.values().stream().map(s -> s.getPeer()).collect(Collectors.toList())));

    builder.setClientRpc(new NettyFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), properties));

    return builder.build();
  }

  private static RaftPeerId peerId(int port) {
    return RaftPeerId.valueOf("p" + port);
  }

  private static String peersInfo(Collection<CServer> peers, String prefix) {
    StringBuilder msgBuilder = new StringBuilder(prefix).append("={");
    if (peers.size() == 0) {
      msgBuilder.append("}");
    } else {
      peers.forEach(p -> msgBuilder.append("\n\t").append(p));
      msgBuilder.append("\n}");
    }
    return msgBuilder.toString();
  }
}
