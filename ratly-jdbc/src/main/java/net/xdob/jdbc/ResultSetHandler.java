package net.xdob.jdbc;

import net.xdob.ratly.util.Timestamp;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ResultSetHandler {
	private final ResultSet resultSet;
	private transient volatile Timestamp lastAccessTime = Timestamp.currentTime();

	public ResultSetHandler(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public ResultSet getResultSet() {
		return resultSet;
	}

	public Timestamp getLastAccessTime() {
		return lastAccessTime;
	}

	public void updateLastAccessTime() {
		this.lastAccessTime = Timestamp.currentTime();
	}

	public void close() throws SQLException {
		resultSet.getStatement().close();
		resultSet.close();
	}
}
