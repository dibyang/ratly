package net.xdob.jdbc.exception;

import net.xdob.ratly.server.exception.SuccessApplied;

import java.sql.SQLInvalidAuthorizationSpecException;

public class AuthorizationFailedException extends SQLInvalidAuthorizationSpecException
		implements SuccessApplied {
	public AuthorizationFailedException() {
		super("User authentication failed", "08004");
	}
}
