
package net.xdob.ratly.shell.cli.sh.group;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import net.xdob.ratly.proto.RaftProtos;
import net.xdob.ratly.protocol.GroupInfoReply;
import net.xdob.ratly.shell.cli.sh.command.AbstractRatlyCommand;
import net.xdob.ratly.shell.cli.sh.command.Context;

import java.io.IOException;

/**
 * Command for querying ratly group information.
 */
public class GroupInfoCommand extends AbstractRatlyCommand {
  /**
   * @param context command context
   */
  public GroupInfoCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "info";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    super.run(cl);
    println("group id: " + getRaftGroup().getGroupId().getUuid());
    final GroupInfoReply reply = getGroupInfoReply();
    RaftProtos.RaftPeerProto leader = getLeader(reply.getRoleInfoProto());
    if (leader == null) {
      println("leader not found");
    } else {
      printf("leader info: %s(%s)%n%n", leader.getId().toStringUtf8(), leader.getAddress());
    }
    println(reply.getCommitInfos());
    println(reply.getLogInfoProto());
    return 0;
  }

  @Override
  public String getUsage() {
    return String.format("%s"
        + " -%s <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT>"
        + " [-%s <RAFT_GROUP_ID>]",
        getCommandName(), PEER_OPTION_NAME, GROUPID_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public Options getOptions() {
    return super.getOptions();
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Display the information of a specific raft group";
  }
}
