package net.xdob.ratly.jdbc.exception;

import net.xdob.ratly.server.exception.SuccessApplied;

import java.sql.SQLNonTransientException;

public class SessionAlreadyClosedException extends SQLNonTransientException
		implements SuccessApplied {
	private static final String SQL_STATE = "08003";
	public SessionAlreadyClosedException(String sessionId, String reason) {
		super("session " + sessionId + " is already closed,"
				+ "reason:" + reason, SQL_STATE);
	}
}
