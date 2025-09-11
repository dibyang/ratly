package net.xdob.jdbc.exception;

import java.sql.SQLNonTransientException;

public class TooManySessionException extends SQLNonTransientException {

  public TooManySessionException() {
    super("too many sessions.", "08001");
  }
}
