package net.xdob.ratly.jdbc.exception;

import java.sql.SQLNonTransientException;

public class NoDatabaseException extends SQLNonTransientException {


  public NoDatabaseException(String db) {
    super("Database not find:"+ db);
  }
}
