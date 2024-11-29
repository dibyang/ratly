package net.xdob.ratly.jdbc.sql;

import net.xdob.ratly.jdbc.Version;

import java.sql.Driver;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public abstract class AbstractDriver implements Driver
{
	/**
	 * {@inheritDoc}
	 * @see Driver#acceptsURL(String)
	 */
	@Override
	public boolean acceptsURL(String url)
	{
		return (this.parse(url) != null);
	}
	
	protected abstract Pattern getUrlPattern();
	
	protected String parse(String url)
	{
		Matcher matcher = this.getUrlPattern().matcher(url);
		
		if (!matcher.matches())
		{
			return null;
		}
		
		return matcher.group(1);
	}

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
