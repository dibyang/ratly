package net.xdob.ratly.server.exception;


public class DbErrorException extends Exception {


	public DbErrorException(String path) {
		super("DB has some error, path="+path);
	}

	public DbErrorException(String path, Throwable cause) {
		super("DB has some error, path="+path, cause);
	}
}
