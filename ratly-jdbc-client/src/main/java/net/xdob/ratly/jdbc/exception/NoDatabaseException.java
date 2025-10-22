package net.xdob.ratly.jdbc.exception;

import java.sql.SQLNonTransientException;

public class NoDatabaseException extends SQLNonTransientException {
	private static final String SQL_STATE = "3D000";

	public NoDatabaseException(String db) {
    super("database " + db + " doesn't exist.", SQL_STATE);
  }
}
