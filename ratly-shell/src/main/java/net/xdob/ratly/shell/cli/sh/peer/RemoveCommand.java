
package net.xdob.ratly.shell.cli.sh.peer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.proto.RaftProtos.RaftPeerRole;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.shell.cli.sh.command.AbstractRatlyCommand;
import net.xdob.ratly.shell.cli.sh.command.Context;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Command for remove ratly server.
 */
public class RemoveCommand extends AbstractRatlyCommand {

  public static final String ADDRESS_OPTION_NAME = "address";
  public static final String PEER_ID_OPTION_NAME = "peerId";
  /**
   * @param context command context
   */
  public RemoveCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "remove";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    super.run(cl);
    final List<RaftPeerId> ids;
    if (cl.hasOption(PEER_ID_OPTION_NAME)) {
      ids = Arrays.stream(cl.getOptionValue(PEER_ID_OPTION_NAME).split(","))
          .map(RaftPeerId::getRaftPeerId).collect(Collectors.toList());
    } else if (cl.hasOption(ADDRESS_OPTION_NAME)) {
      ids = getIds(cl.getOptionValue(ADDRESS_OPTION_NAME).split(","), (a, b) -> {});
    } else {
      throw new IllegalArgumentException(
          "Both " + PEER_ID_OPTION_NAME + " and " + ADDRESS_OPTION_NAME + " options are missing.");
    }
    try (RaftClient client = newRaftClient()) {
      final List<RaftPeer> peers = getPeerStream(RaftPeerRole.FOLLOWER)
          .filter(raftPeer -> !ids.contains(raftPeer.getId())).collect(Collectors.toList());
      final List<RaftPeer> listeners = getPeerStream(RaftPeerRole.LISTENER)
          .filter(raftPeer -> !ids.contains(raftPeer.getId())).collect(Collectors.toList());
      System.out.println("New peer list: " + peers);
      System.out.println("New listener list:  " + listeners);
      final RaftClientReply reply = client.admin().setConfiguration(peers, listeners);
      processReply(reply, () -> "Failed to change raft peer");
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return String.format("%s"
            + " -%s <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT>"
            + " [-%s <RAFT_GROUP_ID>]"
            + " <[-%s <PEER0_HOST:PEER0_PORT>]|[-%s <peerId>]>",
        getCommandName(), PEER_OPTION_NAME, GROUPID_OPTION_NAME,
        ADDRESS_OPTION_NAME, PEER_ID_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public Options getOptions() {
    return super.getOptions()
        .addOption(Option.builder()
            .option(ADDRESS_OPTION_NAME)
            .hasArg()
            .desc("The address information of ratly peers")
            .build())
        .addOption(Option.builder()
            .option(PEER_ID_OPTION_NAME).hasArg()
            .desc("The peer id of ratly peers")
            .build());
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Remove peers to a ratly group";
  }
}
