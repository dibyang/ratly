
package net.xdob.ratly.server.raftlog;

import net.xdob.ratly.proto.raft.*;
import net.xdob.ratly.protocol.ClientId;
import net.xdob.ratly.protocol.RaftClientRequest;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.server.RaftConfiguration;
import net.xdob.ratly.server.RaftConfigurationImpl;
import net.xdob.ratly.server.protocol.TermIndex;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import net.xdob.ratly.util.Preconditions;
import net.xdob.ratly.util.ProtoUtils;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Log proto utilities. */
public final class LogProtoUtils {
  private LogProtoUtils() {}

  public static String toLogEntryString(LogEntryProto entry, Function<StateMachineLogEntryProto, String> function) {
    if (entry == null) {
      return null;
    }
    final String s;
    if (entry.hasStateMachineLogEntry()) {
      if (function == null) {
        function = LogProtoUtils::stateMachineLogEntryProtoToString;
      }
      s = ", " + function.apply(entry.getStateMachineLogEntry());
    } else if (entry.hasMetadataEntry()) {
      final MetadataProto metadata = entry.getMetadataEntry();
      s = "(c:" + metadata.getCommitIndex() + ")";
    } else if (entry.hasConfigurationEntry()) {
      final RaftConfigurationProto config = entry.getConfigurationEntry();
      s = "(current:" + peersToString(config.getPeersList())
          + ", old:" + peersToString(config.getOldPeersList()) + ")";
    } else {
      s = "";
    }
    return TermIndex.valueOf(entry) + ", " + entry.getLogEntryBodyCase() + s;
  }

  static String peersToString(List<RaftPeerProto> peers) {
    return peers.stream().map(AbstractMessage::toString)
        .map(s -> s.replace("\n", ""))
        .map(s -> s.replace(" ", ""))
        .collect(Collectors.joining(", "));
  }

  static String stateMachineLogEntryProtoToString(StateMachineLogEntryProto p) {
    final StateMachineEntryProto stateMachineEntry = p.getStateMachineEntry();
    return p.getType()
        + ": logData.size=" + p.getLogData().size()
        + ", stateMachineData.size=" + stateMachineEntry.getStateMachineData().size()
        + ", logEntryProtoSerializedSize=" + stateMachineEntry.getLogEntryProtoSerializedSize();
  }

  public static String toLogEntryString(LogEntryProto entry) {
    return toLogEntryString(entry, LogProtoUtils::stateMachineLogEntryProtoToString);
  }

  public static String toLogEntriesString(List<LogEntryProto> entries) {
    return entries == null ? null
        : entries.stream().map(LogProtoUtils::toLogEntryString).collect(Collectors.toList()).toString();
  }

  public static String toLogEntriesShortString(List<LogEntryProto> entries,
      Function<StateMachineLogEntryProto, String> stateMachineToString) {
    return entries == null ? null
        : entries.isEmpty()? "<empty>"
        : "size=" + entries.size() + ", first=" + toLogEntryString(entries.get(0), stateMachineToString);
  }

  public static LogEntryProto toLogEntryProto(RaftConfiguration conf, Long term, long index) {
    final LogEntryProto.Builder b = LogEntryProto.newBuilder();
    Optional.ofNullable(term).ifPresent(b::setTerm);
    return b.setIndex(index)
        .setConfigurationEntry(toRaftConfigurationProtoBuilder(conf))
        .build();
  }

  public static RaftConfigurationProto.Builder toRaftConfigurationProtoBuilder(RaftConfiguration conf) {
    return RaftConfigurationProto.newBuilder()
        .addAllPeers(ProtoUtils.toRaftPeerProtos(conf.getCurrentPeers()))
        .addAllListeners(ProtoUtils.toRaftPeerProtos(conf.getCurrentPeers(RaftPeerRole.LISTENER)))
        .addAllOldPeers(ProtoUtils.toRaftPeerProtos(conf.getPreviousPeers()))
        .addAllOldListeners(
            ProtoUtils.toRaftPeerProtos(conf.getPreviousPeers(RaftPeerRole.LISTENER)));
  }

  public static LogEntryProto toLogEntryProto(StateMachineLogEntryProto proto, long term, long index) {
    return LogEntryProto.newBuilder()
        .setTerm(term)
        .setIndex(index)
        .setStateMachineLogEntry(proto)
        .build();
  }

  public static LogEntryProto toLogEntryProto(long commitIndex, long term, long index) {
    return LogEntryProto.newBuilder()
        .setTerm(term)
        .setIndex(index)
        .setMetadataEntry(MetadataProto.newBuilder().setCommitIndex(commitIndex))
        .build();
  }

  /**
   * If the given entry has state machine log entry and it has state machine data,
   * build a new entry without the state machine data.
   *
   * @return a new entry without the state machine data if the given has state machine data;
   *         otherwise, return the given entry.
   */
  public static LogEntryProto removeStateMachineData(LogEntryProto entry) {
    return getStateMachineEntry(entry)
        .map(StateMachineEntryProto::getStateMachineData)
        .filter(stateMachineData -> !stateMachineData.isEmpty())
        .map(_dummy -> replaceStateMachineDataWithSerializedSize(entry))
        .orElse(entry);
  }

  private static LogEntryProto replaceStateMachineDataWithSerializedSize(LogEntryProto entry) {
    LogEntryProto replaced = replaceStateMachineEntry(entry,
        StateMachineEntryProto.newBuilder().setLogEntryProtoSerializedSize(entry.getSerializedSize()));
    return copy(replaced);
  }

  private static LogEntryProto replaceStateMachineEntry(LogEntryProto proto, StateMachineEntryProto.Builder newEntry) {
    Preconditions.assertTrue(proto.hasStateMachineLogEntry(), () -> "Unexpected proto " + proto);
    return LogEntryProto.newBuilder(proto).setStateMachineLogEntry(
        StateMachineLogEntryProto.newBuilder(proto.getStateMachineLogEntry()).setStateMachineEntry(newEntry)
    ).build();
  }

  /**
   * Return a new log entry based on the input log entry with stateMachineData added.
   * @param stateMachineData - state machine data to be added
   * @param entry - log entry to which stateMachineData needs to be added
   * @return LogEntryProto with stateMachineData added
   */
  static LogEntryProto addStateMachineData(ByteString stateMachineData, LogEntryProto entry) {
    Preconditions.assertTrue(isStateMachineDataEmpty(entry),
        () -> "Failed to addStateMachineData to " + entry + " since shouldReadStateMachineData is false.");
    return replaceStateMachineEntry(entry, StateMachineEntryProto.newBuilder().setStateMachineData(stateMachineData));
  }

  public static boolean hasStateMachineData(LogEntryProto entry) {
    return getStateMachineEntry(entry)
        .map(StateMachineEntryProto::getStateMachineData)
        .map(data -> !data.isEmpty())
        .orElse(false);
  }

  public static boolean isStateMachineDataEmpty(LogEntryProto entry) {
    return getStateMachineEntry(entry)
        .map(StateMachineEntryProto::getStateMachineData)
        .map(ByteString::isEmpty)
        .orElse(false);
  }

  private static Optional<StateMachineEntryProto> getStateMachineEntry(LogEntryProto entry) {
    return Optional.of(entry)
        .filter(LogEntryProto::hasStateMachineLogEntry)
        .map(LogEntryProto::getStateMachineLogEntry)
        .filter(StateMachineLogEntryProto::hasStateMachineEntry)
        .map(StateMachineLogEntryProto::getStateMachineEntry);
  }

  public static int getSerializedSize(LogEntryProto entry) {
    return getStateMachineEntry(entry)
        .filter(stateMachineEntry -> stateMachineEntry.getStateMachineData().isEmpty())
        .map(StateMachineEntryProto::getLogEntryProtoSerializedSize)
        .orElseGet(entry::getSerializedSize);
  }

  private static StateMachineLogEntryProto.Type toStateMachineLogEntryProtoType(RaftClientRequestProto.TypeCase type) {
    switch (type) {
      case WRITE: return StateMachineLogEntryProto.Type.WRITE;
      case DATASTREAM: return StateMachineLogEntryProto.Type.DATASTREAM;
      default:
        throw new IllegalStateException("Unexpected request type " + type);
    }
  }

  public static StateMachineLogEntryProto toStateMachineLogEntryProto(
      RaftClientRequest request, ByteString logData, ByteString stateMachineData) {
    if (logData == null) {
      logData = request.getMessage().getContent();
    }
    final StateMachineLogEntryProto.Type type = toStateMachineLogEntryProtoType(request.getType().getTypeCase());
    return toStateMachineLogEntryProto(request.getClientId(), request.getCallId(), type, logData, stateMachineData);
  }

  public static StateMachineLogEntryProto toStateMachineLogEntryProto(ClientId clientId, long callId,
      StateMachineLogEntryProto.Type type, ByteString logData, ByteString stateMachineData) {
    final StateMachineLogEntryProto.Builder b = StateMachineLogEntryProto.newBuilder()
        .setClientId(clientId.toByteString())
        .setCallId(callId)
        .setType(type)
        .setLogData(logData);
    Optional.ofNullable(stateMachineData)
        .map(StateMachineEntryProto.newBuilder()::setStateMachineData)
        .ifPresent(b::setStateMachineEntry);
    return b.build();
  }

  public static RaftConfiguration toRaftConfiguration(LogEntryProto entry) {
    Preconditions.assertTrue(entry.hasConfigurationEntry());
    final RaftConfigurationProto proto = entry.getConfigurationEntry();
    final List<RaftPeer> conf = ProtoUtils.toRaftPeers(proto.getPeersList());
    final List<RaftPeer> listener = ProtoUtils.toRaftPeers(proto.getListenersList());
    final List<RaftPeer> oldConf = ProtoUtils.toRaftPeers(proto.getOldPeersList());
    final List<RaftPeer> oldListener = ProtoUtils.toRaftPeers(proto.getOldListenersList());
    return newRaftConfiguration(conf, listener, entry.getIndex(), oldConf, oldListener);
  }

  public static RaftConfiguration newRaftConfiguration(List<RaftPeer> conf, List<RaftPeer> listener,
                                                       long index, List<RaftPeer> oldConf, List<RaftPeer> oldListener) {
    final RaftConfigurationImpl.Builder b = RaftConfigurationImpl.newBuilder()
        .setConf(conf, listener)
        .setLogEntryIndex(index);
    if (!oldConf.isEmpty() || !oldListener.isEmpty()) {
      b.setOldConf(oldConf, oldListener);
    }
    return b.build();
  }

  public static LogEntryProto copy(LogEntryProto proto) {
    if (proto == null) {
      return null;
    }

    if (!proto.hasStateMachineLogEntry() && !proto.hasMetadataEntry() && !proto.hasConfigurationEntry()) {
      // empty entry, just return as is.
      return proto;
    }

    try {
      return LogEntryProto.parseFrom(proto.toByteString());
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("Failed to copy log entry " + TermIndex.valueOf(proto), e);
    }
  }
}
