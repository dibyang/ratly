package net.xdob.ratly.server.storage;

/**
 * 存储的启动选项
 */
public enum StartupOption {
  /**
   * 表示格式化存储。这通常在存储初始化时使用，清空现有数据并重新创建存储结构。
   */
  FORMAT,
  /**
   * 表示恢复存储。这通常用于从现有存储中恢复数据，处理日志异常等情况。
   */
  RECOVER
}
