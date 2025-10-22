package net.xdob.ratly.jdbc.exception;

import java.sql.SQLNonTransientException;

public class MaxDatabaseExceededException extends SQLNonTransientException {
	private final int currentCount;
	private final int maxAllowed;
	private static final String SQL_STATE = "54000"; // 程序限制违例的SQL状态码

	public MaxDatabaseExceededException(int currentCount, int maxAllowed) {
		super(
				String.format("maximum database count exceeded: current %d, maximum allowed %d", currentCount, maxAllowed),
				SQL_STATE
		);
		this.currentCount = currentCount;
		this.maxAllowed = maxAllowed;
	}

	public int getCurrentCount() {
		return currentCount;
	}

	public int getMaxAllowed() {
		return maxAllowed;
	}
}
