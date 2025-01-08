package net.xdob.ratly.statemachine.impl;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.storage.FileInfo;
import net.xdob.ratly.server.storage.RaftStorage;
import net.xdob.ratly.statemachine.SnapshotInfo;
import net.xdob.ratly.statemachine.TransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface SMPlugin extends Closeable {
  Logger LOG = LoggerFactory.getLogger(SMPlugin.class);
  String getId();
  void initialize(RaftServer server, RaftGroupId groupId,
                  RaftStorage raftStorage, SMPluginContext context) throws IOException;
  default void reinitialize() throws IOException{

  }
  default CompletableFuture<Message> query(Message request){
    return CompletableFuture.completedFuture(null);
  }

  default CompletableFuture<Message> applyTransaction(TermIndex termIndex, ByteString msg){
    return CompletableFuture.completedFuture(null);
  }

  default List<FileInfo> takeSnapshot(FileListStateMachineStorage storage, TermIndex last) throws IOException{
    return Lists.newArrayList();
  }

  default void restoreFromSnapshot(SnapshotInfo snapshot) throws IOException{

  }
}
