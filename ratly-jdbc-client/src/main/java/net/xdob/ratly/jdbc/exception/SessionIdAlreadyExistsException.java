package net.xdob.ratly.jdbc.exception;

import net.xdob.ratly.server.exception.SuccessApplied;

import java.sql.SQLNonTransientException;

public class SessionIdAlreadyExistsException extends SQLNonTransientException
		implements SuccessApplied {


  public SessionIdAlreadyExistsException(String sessionId) {
    super("Session ID already exists: " + sessionId);
  }
}
