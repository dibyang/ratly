package net.xdob.jdbc.exception;

import java.sql.SQLNonTransientException;

public class NoSessionException extends SQLNonTransientException {


  public NoSessionException(String sessionId) {
    super("Session not find:"+ sessionId);
  }
}
