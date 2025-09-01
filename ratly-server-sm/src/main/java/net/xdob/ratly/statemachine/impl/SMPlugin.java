package net.xdob.ratly.statemachine.impl;

import com.google.common.collect.Lists;
import net.xdob.ratly.proto.sm.WrapReplyProto;
import net.xdob.ratly.proto.sm.WrapRequestProto;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.server.RaftServer;
import net.xdob.ratly.server.protocol.TermIndex;
import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.storage.FileInfo;
import net.xdob.ratly.server.storage.RaftStorage;
import net.xdob.ratly.statemachine.SnapshotInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface SMPlugin extends Closeable {
  Logger LOG = LoggerFactory.getLogger(SMPlugin.class);
  String getId();
  void initialize(RaftServer server, RaftGroupId groupId, RaftPeerId peerId, RaftStorage raftStorage) throws IOException;

  void setSMPluginContext(SMPluginContext context);

  default void reinitialize() throws IOException{

  }

	default void heart(WrapRequestProto request, WrapReplyProto.Builder reply) {

	}

  default void query(WrapRequestProto request, WrapReplyProto.Builder reply) {

  }

  default void applyTransaction(TermIndex termIndex, WrapRequestProto request, WrapReplyProto.Builder reply)  {

  }


  default List<FileInfo> takeSnapshot(FileListStateMachineStorage storage, TermIndex last) throws IOException{
    return Lists.newArrayList();
  }

  default void restoreFromSnapshot(SnapshotInfo snapshot) throws IOException{

  }


  /**
   * 获取插件最新的已应用的日志索引，
   * 用来处理多日志事务，返回INVALID_LOG_INDEX表示该插件不需要阶段性事务(多日志事务)
   * @see RaftLog#INVALID_LOG_INDEX
   * @return 插件事务阶段性索引
   */
  default long getLastPluginAppliedIndex(){
    return RaftLog.INVALID_LOG_INDEX;
  }

}
