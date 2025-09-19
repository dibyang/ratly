package net.xdob.jdbc.exception;

import java.sql.SQLException;

public class SQLParserException extends SQLException {
	public SQLParserException(String reason, Throwable cause) {
		super(reason, cause);
	}
}
