package net.xdob.ratly.io;

/**
 * 标准写选项
 */
public enum StandardWriteOption implements WriteOption {
  /**
   * Sync时刷到底层存储
   * Note that SYNC does not imply {@link #FLUSH}.
   */
  SYNC,
  /**
   * Close时刷到底层存储
   */
  CLOSE,
  /**
   * Flush时清空缓存数据
   */
  FLUSH,
}
