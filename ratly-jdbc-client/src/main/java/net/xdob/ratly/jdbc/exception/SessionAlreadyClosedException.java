package net.xdob.ratly.jdbc.exception;

import net.xdob.ratly.server.exception.SuccessApplied;

import java.sql.SQLNonTransientException;

public class SessionAlreadyClosedException extends SQLNonTransientException
		implements SuccessApplied {

	public SessionAlreadyClosedException(String sessionId, String reason) {
		super("Session is already closed, Session not find:" + sessionId
				+ ". reason:"+reason, "08003");
	}
}
