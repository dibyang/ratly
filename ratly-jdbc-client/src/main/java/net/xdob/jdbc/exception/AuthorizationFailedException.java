package net.xdob.jdbc.exception;

import java.sql.SQLInvalidAuthorizationSpecException;

public class AuthorizationFailedException extends SQLInvalidAuthorizationSpecException {
	public AuthorizationFailedException() {
		super("User authentication failed", "08004");
	}
}
