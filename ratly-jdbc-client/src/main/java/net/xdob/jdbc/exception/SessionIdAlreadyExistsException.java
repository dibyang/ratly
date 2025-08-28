package net.xdob.jdbc.exception;

import java.sql.SQLNonTransientException;

public class SessionIdAlreadyExistsException extends SQLNonTransientException {


  public SessionIdAlreadyExistsException(String sessionId) {
    super("Session ID already exists: " + sessionId);
  }
}
