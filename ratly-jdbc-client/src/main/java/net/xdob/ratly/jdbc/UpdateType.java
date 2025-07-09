package net.xdob.ratly.jdbc;

public enum UpdateType {
  execute,
  commit,
  rollback,
  savepoint,
  releaseSavepoint,
  rollbackSavepoint,
  openSession,
  closeSession
}
