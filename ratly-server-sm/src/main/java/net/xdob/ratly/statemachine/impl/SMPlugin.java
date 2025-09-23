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
import java.util.ArrayList;
import java.util.List;

public interface SMPlugin extends Closeable {
  Logger LOG = LoggerFactory.getLogger(SMPlugin.class);
  String getId();
  void initialize(RaftServer server, RaftGroupId groupId, RaftPeerId peerId, RaftStorage raftStorage) throws IOException;

  void setSMPluginContext(SMPluginContext context);

  default void reinitialize() throws IOException{

  }

	default void admin(WrapRequestProto request, WrapReplyProto.Builder reply) {

	}

  default void query(WrapRequestProto request, WrapReplyProto.Builder reply) {

  }

  default void applyTransaction(TermIndex termIndex, WrapRequestProto request, WrapReplyProto.Builder reply)  {

  }

	/**
	 * 快照生成
	 */

  default List<FileInfo> takeSnapshot(FileListStateMachineStorage storage, TermIndex last) throws IOException{
    return Lists.newArrayList();
  }

	/**
	 * 此方法不再持有读锁
	 * 完成快照的后继处理，比如数据校验
	 */
	default void finishSnapshot(FileListStateMachineStorage storage, TermIndex last, List<FileInfo>  files) throws IOException{
	}

  default void restoreFromSnapshot(SnapshotInfo snapshot) throws IOException{

  }


	/**
	 * 获取插件已结束事务的索引列表
	 */
	default List<Long> getLastEndedTxIndexList(){
		return new ArrayList<>();
	}
	/**
	 * 获取插件内最早事务的索引
	 * @return 插件内最早事务的索引
	 */
	default long getFirstTx(){
		return RaftLog.INVALID_LOG_INDEX;
	}
}
