package net.xdob.ratly.jdbc.exception;

import net.xdob.ratly.server.exception.SuccessApplied;

import java.sql.SQLNonTransientException;

public class DatabaseAlreadyClosedException extends SQLNonTransientException
		implements SuccessApplied {

	public DatabaseAlreadyClosedException(String sessionId) {
		super("Database is already closed, Session not find:" + sessionId, "08003");
	}
}
