package net.xdob.ratly.jdbc;

import net.xdob.ratly.jdbc.sql.*;
import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;


public final class RatlyDriver extends AbstractDriver
{
	public static final String START_URL = "jdbc:ratly-jdbc:";
	public static final String URL_FORMAT = START_URL+":{db}:group={group};peers={peers}";

	private static final Logger logger = LoggerFactory.getLogger(RatlyDriver.class);


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
}
