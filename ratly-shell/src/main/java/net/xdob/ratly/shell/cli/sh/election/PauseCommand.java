
package net.xdob.ratly.shell.cli.sh.election;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.shell.cli.sh.command.AbstractRatlyCommand;
import net.xdob.ratly.shell.cli.sh.command.Context;

import java.io.IOException;

/**
 * Command for pause leader election on specific server
 */
public class PauseCommand extends AbstractRatlyCommand {

  public static final String ADDRESS_OPTION_NAME = "address";
  /**
   * @param context command context
   */
  public PauseCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "pause";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    super.run(cl);

    String strAddr = cl.getOptionValue(ADDRESS_OPTION_NAME);
    final RaftPeerId peerId = getRaftGroup().getPeers().stream()
        .filter(p -> p.getAddress().equals(strAddr)).findAny()
        .map(RaftPeer::getId)
        .orElse(null);
    if (peerId == null) {
      printf("Peer not found: %s", strAddr);
      return -1;
    }
    try(final RaftClient raftClient = newRaftClient()) {
      RaftClientReply reply = raftClient.getLeaderElectionManagementApi(peerId).pause();
      processReply(reply, () -> String.format("Failed to pause leader election on peer %s", strAddr));
      printf(String.format("Successful pause leader election on peer %s", strAddr));
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return String.format("%s -%s <HOSTNAME:PORT>"
            + " -%s <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT>"
            + " [-%s <RAFT_GROUP_ID>]",
        getCommandName(), ADDRESS_OPTION_NAME, PEER_OPTION_NAME,
        GROUPID_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public Options getOptions() {
    return super.getOptions().addOption(
        Option.builder()
            .option(ADDRESS_OPTION_NAME)
            .hasArg()
            .required()
            .desc("Server address that will be paused its leader election")
            .build()
    );
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Pause leader election to the server <hostname>:<port>";
  }
}
