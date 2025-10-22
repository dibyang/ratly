package net.xdob.ratly.jdbc.exception;

import net.xdob.ratly.server.exception.SuccessApplied;

import java.sql.SQLNonTransientException;

public class TooManySessionException extends SQLNonTransientException
		implements SuccessApplied {
	private static final String SQL_STATE = "08001";
	public TooManySessionException() {
		super("too many sessions.", SQL_STATE);
	}
}
