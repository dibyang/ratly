
package net.xdob.ratly.shell.cli.sh.election;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.proto.raft.RaftPeerRole;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.exceptions.TransferLeadershipException;
import net.xdob.ratly.shell.cli.sh.command.AbstractRatlyCommand;
import net.xdob.ratly.shell.cli.sh.command.Context;
import net.xdob.ratly.util.TimeDuration;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Command for transferring the ratly leader to specific server.
 */
public class TransferCommand extends AbstractRatlyCommand {
  public static final String ADDRESS_OPTION_NAME = "address";
  public static final String TIMEOUT_OPTION_NAME = "timeout";
  /**
   * @param context command context
   */
  public TransferCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "transfer";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    super.run(cl);

    String strAddr = cl.getOptionValue(ADDRESS_OPTION_NAME);
    // Default timeout is 0, which means let server decide (will use default request timeout).
    final TimeDuration timeoutDefault = TimeDuration.ZERO;
    // Default timeout for legacy mode matches with the legacy command (version 2.4.x and older).
    final TimeDuration timeoutLegacy = TimeDuration.valueOf(60, TimeUnit.SECONDS);
    final Optional<TimeDuration> timeout = !cl.hasOption(TIMEOUT_OPTION_NAME) ? Optional.empty() :
        Optional.of(TimeDuration.valueOf(cl.getOptionValue(TIMEOUT_OPTION_NAME), TimeUnit.SECONDS));

    final int highestPriority = getRaftGroup().getPeers().stream()
        .mapToInt(RaftPeer::getPriority).max().orElse(0);
    RaftPeer newLeader = getRaftGroup().getPeers().stream()
        .filter(peer -> peer.getAddress().equals(strAddr)).findAny().orElse(null);
    if (newLeader == null) {
      printf("Peer with address %s not found.", strAddr);
      return -2;
    }
    try (RaftClient client = newRaftClient()) {
      // transfer leadership
      if (!tryTransfer(client, newLeader, highestPriority, timeout.orElse(timeoutDefault))) {
        // legacy mode, transfer leadership by setting priority.
        tryTransfer(client, newLeader, highestPriority + 1, timeout.orElse(timeoutLegacy));
      }
    } catch (Throwable t) {
      printf("Failed to transfer to peer %s with address %s: ", newLeader.getId(), newLeader.getAddress());
      t.printStackTrace(getPrintStream());
      return -1;
    }
    return 0;
  }

  private boolean tryTransfer(RaftClient client, RaftPeer newLeader, int highestPriority, TimeDuration timeout)
      throws IOException {
    printf("Transferring leadership to peer %s with address %s%n", newLeader.getId(), newLeader.getAddress());
    try {
      // lift the new leader to the highest priority,
      if (newLeader.getPriority() < highestPriority) {
        setPriority(client, newLeader, highestPriority);
      }
      RaftClientReply transferLeadershipReply =
          client.admin().transferLeadership(newLeader.getId(), timeout.toLong(TimeUnit.MILLISECONDS));
      processReply(transferLeadershipReply, () -> "election failed");
    } catch (TransferLeadershipException tle) {
      if (tle.getMessage().contains("it does not has highest priority")) {
        return false;
      }
      throw tle;
    }
    println("Transferring leadership initiated");
    return true;
  }

  private void setPriority(RaftClient client, RaftPeer target, int priority) throws IOException {
    printf("Changing priority of peer %s with address %s to %d%n", target.getId(), target.getAddress(), priority);
    final List<RaftPeer> peers = getPeerStream(RaftPeerRole.FOLLOWER)
        .map(peer -> peer == target ? RaftPeer.newBuilder(peer).setPriority(priority).build() : peer)
        .collect(Collectors.toList());
    final List<RaftPeer> listeners = getPeerStream(RaftPeerRole.LISTENER).collect(Collectors.toList());
    RaftClientReply reply = client.admin().setConfiguration(peers, listeners);
    processReply(reply, () -> "Failed to set master priorities");
  }

  @Override
  public String getUsage() {
    return String.format("%s -%s <HOSTNAME:PORT>"
            + " -%s <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT>"
            + " [-%s <RAFT_GROUP_ID>] [-%s <TIMEOUT_IN_SECONDS>]",
        getCommandName(), ADDRESS_OPTION_NAME, PEER_OPTION_NAME,
        GROUPID_OPTION_NAME, TIMEOUT_OPTION_NAME);
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
            .desc("Server address that will take over as leader")
            .build()
    ).addOption(
        Option.builder()
            .option(TIMEOUT_OPTION_NAME)
            .hasArg()
            .desc("Timeout for transfer leadership to complete (in seconds)")
            .build()
    );
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Transfers leadership to the <hostname>:<port>";
  }
}
