package net.xdob.ratly.server.exception;


public class DbErrorException extends Exception {


	public DbErrorException(String path) {
		super(path, null);
	}

	public DbErrorException(String path, Throwable cause) {
		super("DB has some error, path="+path, cause);
	}

	public static DbErrorException error(String path, Throwable cause) {
		return new DbErrorException("DB has some error, path="+path, cause);
	}

	public static DbErrorException error(String path) {
		return error(path, null);
	}

	public static DbErrorException notExists(String path, Throwable cause) {
		return new DbErrorException("DB file is not exists, path="+path, cause);
	}

	public static DbErrorException notExists(String path) {
		return notExists(path, null);
	}

}
