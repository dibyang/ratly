
package net.xdob.ratly.shell.cli.sh.peer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.proto.RaftProtos.RaftPeerRole;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.shell.cli.sh.command.AbstractRatlyCommand;
import net.xdob.ratly.shell.cli.sh.command.Context;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SetPriorityCommand extends AbstractRatlyCommand {

  public static final String PEER_WITH_NEW_PRIORITY_OPTION_NAME = "addressPriority";

  /**
   * @param context command context
   */
  public SetPriorityCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "setPriority";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    super.run(cl);
    Map<String, Integer> addressPriorityMap = new HashMap<>();
    for (String optionValue : cl.getOptionValues(PEER_WITH_NEW_PRIORITY_OPTION_NAME)) {
      String[] str = optionValue.split("[|]");
      if (str.length < 2) {
        println("The format of the parameter is wrong");
        return -1;
      }
      addressPriorityMap.put(str[0], Integer.parseInt(str[1]));
    }

    try (RaftClient client = newRaftClient()) {
      final List<RaftPeer> peers = getPeerStream(RaftPeerRole.FOLLOWER).map(peer -> {
        final Integer newPriority = addressPriorityMap.get(peer.getAddress());
        final int priority = newPriority != null ? newPriority : peer.getPriority();
        return RaftPeer.newBuilder(peer).setPriority(priority).build();
      }).collect(Collectors.toList());
      final List<RaftPeer> listeners =
          getPeerStream(RaftPeerRole.LISTENER).collect(Collectors.toList());
      RaftClientReply reply = client.admin().setConfiguration(peers, listeners);
      processReply(reply, () -> "Failed to set master priorities ");
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return String.format("%s"
            + " -%s <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT>"
            + " [-%s <RAFT_GROUP_ID>]"
            + " -%s <PEER_HOST:PEER_PORT|PRIORITY>",
        getCommandName(), PEER_OPTION_NAME, GROUPID_OPTION_NAME, PEER_WITH_NEW_PRIORITY_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public Options getOptions() {
    return super.getOptions().addOption(
        Option.builder()
            .option(PEER_WITH_NEW_PRIORITY_OPTION_NAME)
            .hasArg()
            .required()
            .desc("Peers information with priority")
            .build());
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Set priority to ratly peers";
  }
}
