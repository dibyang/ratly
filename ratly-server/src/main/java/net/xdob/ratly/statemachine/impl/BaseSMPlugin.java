package net.xdob.ratly.statemachine.impl;

import net.xdob.ratly.server.raftlog.RaftLog;
import net.xdob.ratly.server.raftlog.RaftLogIndex;

public abstract class BaseSMPlugin implements SMPlugin{
  private final String pluginId;
  private final RaftLogIndex appliedIndex;

  public BaseSMPlugin(String pluginId) {
    this.pluginId = pluginId;
    appliedIndex = new RaftLogIndex(pluginId+"_AppliedIndex", RaftLog.INVALID_LOG_INDEX);
  }

  @Override
  public String getId() {
    return pluginId;
  }

  /**
   * 事务完成更新插件事务阶段性索引
   * @param newIndex 插件事务阶段性索引
   */
  public boolean updateAppliedIndexToMax(long newIndex) {
    return appliedIndex.updateToMax(newIndex,
        message -> LOG.debug("updateAppliedIndex {}", message));
  }

  @Override
  public long getLastPluginAppliedIndex() {
    return appliedIndex.get();
  }
}
