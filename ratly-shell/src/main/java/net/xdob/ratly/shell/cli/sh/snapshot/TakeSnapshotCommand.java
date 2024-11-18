
package net.xdob.ratly.shell.cli.sh.snapshot;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.shell.cli.sh.command.AbstractRatlyCommand;
import net.xdob.ratly.shell.cli.sh.command.Context;

import java.io.IOException;

/**
 * Command for make a ratly server take snapshot.
 */
public class TakeSnapshotCommand extends AbstractRatlyCommand {
  public static final String TAKE_SNAPSHOT_TIMEOUT_OPTION_NAME = "snapshotTimeout";
  public static final String PEER_ID_OPTION_NAME = "peerId";

  /**
   * @param context command context
   */
  public TakeSnapshotCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "create";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    super.run(cl);
    long timeout;
    final RaftPeerId peerId;
    if (cl.hasOption(TAKE_SNAPSHOT_TIMEOUT_OPTION_NAME)) {
      timeout = Long.parseLong(cl.getOptionValue(TAKE_SNAPSHOT_TIMEOUT_OPTION_NAME));
    } else {
      timeout = 3000;
    }
    try(final RaftClient raftClient = newRaftClient()) {
      if (cl.hasOption(PEER_ID_OPTION_NAME)) {
        peerId = RaftPeerId.getRaftPeerId(cl.getOptionValue(PEER_ID_OPTION_NAME));
      } else {
        peerId = null;
      }
      RaftClientReply reply = raftClient.getSnapshotManagementApi(peerId).create(timeout);
      processReply(reply, () -> String.format("Failed to take snapshot of peerId %s", peerId));
      printf(String.format("Successful take snapshot on peerId %s, the latest snapshot index is %d",
          peerId, reply.getLogIndex()));
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return String.format("%s"
            + " -%s <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT>"
            + " [-%s <RAFT_GROUP_ID>]"
            + " [-%s <timeoutInMs>]"
            + " [-%s <raftPeerId>]",
        getCommandName(),
        PEER_OPTION_NAME,
        GROUPID_OPTION_NAME,
        TAKE_SNAPSHOT_TIMEOUT_OPTION_NAME,
        PEER_ID_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public Options getOptions() {
    return super.getOptions()
        .addOption(Option.builder()
            .option(TAKE_SNAPSHOT_TIMEOUT_OPTION_NAME)
            .hasArg()
            .desc("timeout to wait taking snapshot in ms")
            .build())
        .addOption(Option.builder()
            .option(PEER_ID_OPTION_NAME)
            .hasArg()
            .desc("the id of server takeing snapshot")
            .build());
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Make a ratly server take snapshot";
  }
}
