package net.xdob.ratly.jdbc.sql;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;
import java.util.regex.Pattern;


public final class Driver extends AbstractDriver
{
	private static final Pattern URL_PATTERN = Pattern.compile("jdbc:ratly-jdbc:(?://)?([^/]+)(?:/.+)?");
	private static final Logger logger = LoggerFactory.getLogger(Driver.class);


	@Override
	protected Pattern getUrlPattern() {
		return URL_PATTERN;
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		return new JdbcConnection();
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
