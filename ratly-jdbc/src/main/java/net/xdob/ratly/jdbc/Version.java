package net.xdob.ratly.jdbc;

import java.util.regex.Pattern;

public class Version
{
	public static final String DASH = "-"; //$NON-NLS-1$
	public static final String DOT = ".";
	public static final Version CURRENT = new Version("1.2");

	private final String version;
	private final int major;
	private final int minor;
	private final int revision;
	
	Version(String version)
	{
		this.version = version;
		this.major = parse(0);
		this.minor = parse(1);
		this.revision = parse(2);
	}
	
	private int parse(int index)
	{
		String[] values = this.version.split(Pattern.quote(DASH))[0].split(Pattern.quote(DOT));
		if(index<values.length) {
			return Integer.parseInt(values[index]);
		}
		return 0;
	}
	
	public int getMajor()
	{
		return this.major;
	}
	
	public int getMinor()
	{
		return this.minor;
	}
	
	public int getRevision()
	{
		return this.revision;
	}
	
	@Override
	public String toString()
	{
		return this.version;
	}

}
