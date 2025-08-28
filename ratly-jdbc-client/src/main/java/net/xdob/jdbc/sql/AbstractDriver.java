package net.xdob.jdbc.sql;

import net.xdob.jdbc.Version;

import java.sql.Driver;


public abstract class AbstractDriver implements Driver
{

	/**
	 * {@inheritDoc}
	 * @see Driver#jdbcCompliant()
	 */
	@Override
	public boolean jdbcCompliant()
	{
		return false;
	}

	/**
	 * @see Driver#getMajorVersion()
	 */
	@Override
	public int getMajorVersion()
	{
		return Version.CURRENT.getMajor();
	}
	
	/**
	 * @see Driver#getMinorVersion()
	 */
	@Override
	public int getMinorVersion()
	{
		return Version.CURRENT.getMinor();
	}
}
