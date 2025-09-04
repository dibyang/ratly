package net.xdob.jdbc.exception;

import java.sql.SQLNonTransientException;

public class DatabaseAlreadyClosedException extends SQLNonTransientException {

  public DatabaseAlreadyClosedException(String sessionId) {
    super("Database is already closed, Session not find:"+ sessionId, "08003");
  }
}
