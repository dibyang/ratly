package net.xdob.jdbc.exception;

import net.xdob.ratly.server.exception.SuccessApplied;

import java.sql.SQLNonTransientException;

public class TooManySessionException extends SQLNonTransientException
		implements SuccessApplied {

	public TooManySessionException() {
		super("too many sessions.", "08001");
	}
}
