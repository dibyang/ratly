package net.xdob.ratly.server.storage;

/**
 * 存储状态
 */
public enum StorageState {
  /**
   * 未初始化。
   */
  UNINITIALIZED,
  /**
   * 存储正常。
   */
  NORMAL,
  /**
   * 目录不存在。
   */
  NON_EXISTENT,
  /**
   * 目录存在但未格式化。
   */
  NOT_FORMATTED,
  /**
   * 目录没有足够的空间。
   */
  NO_SPACE
}
