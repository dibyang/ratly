package net.xdob.ratly.jdbc.exception;

import net.xdob.ratly.server.exception.SuccessApplied;

import java.sql.SQLException;

public class SQLParserException extends SQLException
		implements SuccessApplied {
	public SQLParserException(String reason, Throwable cause) {
		super(reason, cause);
	}
}
