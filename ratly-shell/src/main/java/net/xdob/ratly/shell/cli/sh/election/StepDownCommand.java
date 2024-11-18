
package net.xdob.ratly.shell.cli.sh.election;

import org.apache.commons.cli.CommandLine;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.shell.cli.sh.command.AbstractRatlyCommand;
import net.xdob.ratly.shell.cli.sh.command.Context;

import java.io.IOException;

/**
 * Command for stepping down ratly leader server.
 */
public class StepDownCommand extends AbstractRatlyCommand {

  /**
   * @param context command context
   */
  public StepDownCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "stepDown";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    super.run(cl);

    try (RaftClient client = newRaftClient()) {
      RaftPeerId leaderId = RaftPeerId.valueOf(getLeader(getGroupInfoReply().getRoleInfoProto()).getId());
      final RaftClientReply transferLeadershipReply = client.admin().transferLeadership(null, leaderId, 60_000);
      processReply(transferLeadershipReply, () -> "Failed to step down leader");
    } catch (Throwable t) {
      printf("caught an error when executing step down leader: %s%n", t.getMessage());
      return -1;
    }
    println("Step down leader successfully");
    return 0;
  }

  @Override
  public String getUsage() {
    return String.format("%s -%s <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT>"
            + " [-%s <RAFT_GROUP_ID>]",
        getCommandName(), PEER_OPTION_NAME, GROUPID_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return description();
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Step down the leader server.";
  }
}
