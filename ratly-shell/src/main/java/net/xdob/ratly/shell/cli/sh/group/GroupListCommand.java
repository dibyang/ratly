
package net.xdob.ratly.shell.cli.sh.group;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.protocol.GroupListReply;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.shell.cli.CliUtils;
import net.xdob.ratly.shell.cli.sh.command.AbstractRatlyCommand;
import net.xdob.ratly.shell.cli.sh.command.Context;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Command for querying the group information of a ratly server.
 */
public class GroupListCommand extends AbstractRatlyCommand {
  public static final String SERVER_ADDRESS_OPTION_NAME = "serverAddress";
  public static final String PEER_ID_OPTION_NAME = "peerId";

  /**
   * @param context command context
   */
  public GroupListCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "list";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    super.run(cl);
    final RaftPeerId peerId;
    final String address;

    if (cl.hasOption(PEER_ID_OPTION_NAME)) {
      peerId = RaftPeerId.getRaftPeerId(cl.getOptionValue(PEER_ID_OPTION_NAME));
      address = getRaftGroup().getPeer(peerId).getAddress();
    } else if (cl.hasOption(SERVER_ADDRESS_OPTION_NAME)) {
      address = cl.getOptionValue(SERVER_ADDRESS_OPTION_NAME);
      final InetSocketAddress serverAddress = CliUtils.parseInetSocketAddress(address);
      peerId = CliUtils.getPeerId(serverAddress);
    } else {
      throw new IllegalArgumentException(
              "Both " + PEER_ID_OPTION_NAME + " and " + SERVER_ADDRESS_OPTION_NAME
              + " options are missing.");
    }

    try(final RaftClient raftClient = newRaftClient()) {
      GroupListReply reply = raftClient.getGroupManagementApi(peerId).list();
      processReply(reply, () -> String.format("Failed to get group information of peerId %s (server %s)",
              peerId, address));
      printf(String.format("The peerId %s (server %s) is in %d groups, and the groupIds is: %s",
              peerId, address, reply.getGroupIds().size(), reply.getGroupIds()));
    }
    return 0;

  }

  @Override
  public String getUsage() {
    return String.format("%s"
                    + " -%s <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT>"
                    + " [-%s <RAFT_GROUP_ID>]"
                    + " <[-%s <PEER0_HOST:PEER0_PORT>]|[-%s <peerId>]>",
            getCommandName(), PEER_OPTION_NAME, GROUPID_OPTION_NAME, SERVER_ADDRESS_OPTION_NAME,
            PEER_ID_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public Options getOptions() {
    OptionGroup group = new OptionGroup();
    group.setRequired(true);
    group.addOption(new Option(null, SERVER_ADDRESS_OPTION_NAME, true, "the server address"));
    group.addOption(new Option(null, PEER_ID_OPTION_NAME, true, "the peer id"));
    return super.getOptions().addOptionGroup(group);
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Display the group information of a specific raft server";
  }
}
