package net.xdob.ratly.jdbc;

import net.xdob.jdbc.sql.AbstractDriver;
import net.xdob.jdbc.sql.JdbcConnection;
import net.xdob.jdbc.sql.JdbcConnectionInfo;
import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;


public final class Driver extends AbstractDriver
{

	private static final Logger logger = LoggerFactory.getLogger(Driver.class);
	static final Driver INSTANCE = new Driver();
	public static final String START_URL = "jdbc:ratly:";
	public static final String URL_FORMAT = START_URL+"//{group}/{db}?peers={peers}&port={port}&vport={vport}";

	private static boolean registered;

	static {
		load();
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		if (url == null) {
			throw DbException.getJdbcSQLException(ErrorCode.URL_FORMAT_ERROR_2, null, URL_FORMAT, null);
		} else if (url.startsWith(START_URL)) {
			JdbcConnectionInfo ci = new JdbcConnectionInfo(url, info);
			return new JdbcConnection(ci);
		}  else {
			return null;
		}
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		if (url == null) {
			throw DbException.getJdbcSQLException(ErrorCode.URL_FORMAT_ERROR_2, null, URL_FORMAT, null);
		} else {
			return url.startsWith(START_URL);
		}
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		return new DriverPropertyInfo[0];
	}

	@Override
	public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return null;
	}

	public static synchronized Driver load() {
		try {
			if (!registered) {
				registered = true;
				DriverManager.registerDriver(INSTANCE);
			}
		} catch (SQLException e) {
			DbException.traceThrowable(e);
		}
		return INSTANCE;
	}

	/**
	 * INTERNAL
	 */
	public static synchronized void unload() {
		try {
			if (registered) {
				registered = false;
				DriverManager.deregisterDriver(INSTANCE);
			}
		} catch (SQLException e) {
			DbException.traceThrowable(e);
		}
	}
}
