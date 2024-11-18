
package net.xdob.ratly.examples.debug.server;

import net.xdob.ratly.examples.common.Constants;
import net.xdob.ratly.examples.counter.server.CounterServer;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.util.TimeDuration;

import java.io.File;
import java.io.IOException;

/**
 * For running a {@link CounterServer}.
 */
public final class Server {

  private Server(){
  }

  @SuppressWarnings({"squid:S2095"}) // Suppress closeable  warning
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("The arguments should be <ip:port>");
      System.exit(1);
    }

    //find current peer object based on application parameter
    final RaftPeer currentPeer = Constants.PEERS.stream()
        .filter(raftPeer -> raftPeer.getAddress().equals(args[0]))
        .findFirst().orElseThrow(() -> new IllegalArgumentException("Peer not found: " + args[0]));

    final File storageDir = new File(Constants.PATH, currentPeer.getId().toString());
    final CounterServer counterServer = new CounterServer(currentPeer, storageDir, TimeDuration.ZERO);
    counterServer.start();
  }
}
