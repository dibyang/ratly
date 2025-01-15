package net.xdob.ratly.statemachine.impl;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.RaftLog;
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

  /**
   * 事务完成更新插件事务阶段性索引
   * @param newIndex 插件事务阶段性索引
   */
  boolean updateAppliedIndexToMax(long newIndex);

  /**
   * 获取插件最新的已应用的日志索引，
   * 用来处理多日志事务，返回INVALID_LOG_INDEX表示该插件不需要阶段性事务(多日志事务)
   * @see RaftLog#INVALID_LOG_INDEX
   * @return 插件事务阶段性索引
   */
  long getLastPluginAppliedIndex();
}
