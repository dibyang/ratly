
package net.xdob.ratly.shell.cli.sh.command;

import org.apache.commons.cli.Option;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftGroup;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.protocol.GroupInfoReply;
import net.xdob.ratly.shell.cli.CliUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.proto.RaftProtos.RaftConfigurationProto;
import net.xdob.ratly.proto.RaftProtos.FollowerInfoProto;
import net.xdob.ratly.proto.RaftProtos.RaftPeerProto;
import net.xdob.ratly.proto.RaftProtos.RaftPeerRole;
import net.xdob.ratly.proto.RaftProtos.RoleInfoProto;
import net.xdob.ratly.util.ProtoUtils;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The base class for the ratly shell which need to connect to server.
 */
public abstract class AbstractRatlyCommand extends AbstractCommand {
  public static final String PEER_OPTION_NAME = "peers";
  public static final String GROUPID_OPTION_NAME = "groupid";
  private RaftGroup raftGroup;
  private GroupInfoReply groupInfoReply;

  protected AbstractRatlyCommand(Context context) {
    super(context);
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    final List<RaftPeer> peers = CliUtils.parseRaftPeers(cl.getOptionValue(PEER_OPTION_NAME));
    final RaftGroupId groupIdSpecified = CliUtils.parseRaftGroupId(cl.getOptionValue(GROUPID_OPTION_NAME));
    raftGroup = RaftGroup.valueOf(groupIdSpecified != null? groupIdSpecified: RaftGroupId.randomId(), peers);
    PrintStream printStream = getPrintStream();
    try (final RaftClient client = newRaftClient()) {
      final RaftGroupId remoteGroupId = CliUtils.getGroupId(client, peers, groupIdSpecified, printStream);
      groupInfoReply = CliUtils.getGroupInfo(client, peers, remoteGroupId, printStream);
      raftGroup = groupInfoReply.getGroup();
    }
    return 0;
  }

  protected RaftClient newRaftClient() {
    return getContext().newRaftClient(getRaftGroup());
  }

  @Override
  public Options getOptions() {
    return new Options()
            .addOption(
                Option.builder()
                    .option(PEER_OPTION_NAME)
                    .hasArg()
                    .required()
                    .desc("Peer addresses seperated by comma")
                    .build())
            .addOption(GROUPID_OPTION_NAME, true, "Raft group id");
  }

  protected RaftGroup getRaftGroup() {
    return raftGroup;
  }

  protected GroupInfoReply getGroupInfoReply() {
    return groupInfoReply;
  }

  /**
   * Get the leader id.
   *
   * @param roleInfo the role info
   * @return the leader id
   */
  protected RaftPeerProto getLeader(RoleInfoProto roleInfo) {
    if (roleInfo == null) {
      return null;
    }
    if (roleInfo.getRole() == RaftPeerRole.LEADER) {
      return roleInfo.getSelf();
    }
    FollowerInfoProto followerInfo = roleInfo.getFollowerInfo();
    if (followerInfo == null) {
      return null;
    }
    return followerInfo.getLeaderInfo().getId();
  }

  protected void processReply(RaftClientReply reply, Supplier<String> messageSupplier) throws IOException {
    CliUtils.checkReply(reply, messageSupplier, getPrintStream());
  }

  protected List<RaftPeerId> getIds(String[] optionValues, BiConsumer<RaftPeerId, InetSocketAddress> consumer) {
    if (optionValues == null) {
      return Collections.emptyList();
    }
    final List<RaftPeerId> ids = new ArrayList<>();
    for (String address : optionValues) {
      final InetSocketAddress serverAddress = CliUtils.parseInetSocketAddress(address);
      final RaftPeerId peerId = CliUtils.getPeerId(serverAddress);
      consumer.accept(peerId, serverAddress);
      ids.add(peerId);
    }
    return ids;
  }

  protected Stream<RaftPeer> getPeerStream(RaftPeerRole role) {
    final RaftConfigurationProto conf = groupInfoReply.getConf().orElse(null);
    if (conf == null) {
      // Assume all peers are followers in order preserve the pre-listener behaviors.
      return role == RaftPeerRole.FOLLOWER ? getRaftGroup().getPeers().stream() : Stream.empty();
    }
    final Set<RaftPeer> targets = (role == RaftPeerRole.LISTENER ? conf.getListenersList() : conf.getPeersList())
        .stream()
        .map(ProtoUtils::toRaftPeer)
        .collect(Collectors.toSet());
    return getRaftGroup()
        .getPeers()
        .stream()
        .filter(targets::contains);
  }
}
