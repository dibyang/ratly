package net.xdob.ratly.server.exception;

public class SnapshotException extends RuntimeException
		implements MustStopNode{
	public SnapshotException(String message) {
		super(message);
	}

	public SnapshotException(String message, Throwable cause) {
		super(message, cause);
	}

	public SnapshotException(Throwable cause) {
		super(cause);
	}
}
