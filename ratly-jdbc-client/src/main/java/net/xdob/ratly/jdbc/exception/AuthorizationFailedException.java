package net.xdob.ratly.jdbc.exception;

import net.xdob.ratly.server.exception.SuccessApplied;

import java.sql.SQLInvalidAuthorizationSpecException;

public class AuthorizationFailedException extends SQLInvalidAuthorizationSpecException
		implements SuccessApplied {
	private static final String SQL_STATE = "08004";
	public AuthorizationFailedException() {
		super("user authentication failed", SQL_STATE);
	}
}
