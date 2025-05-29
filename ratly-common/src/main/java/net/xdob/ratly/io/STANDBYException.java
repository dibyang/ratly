package net.xdob.ratly.io;

import java.io.IOException;

public class STANDBYException extends IOException {
	public STANDBYException() {
		super("node is STANDBY");
	}

	public STANDBYException(Throwable cause) {
		super("node is STANDBY", cause);
	}
}
