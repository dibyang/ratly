package net.xdob.ratly.protocol;

import java.nio.charset.StandardCharsets;

public class RowId2 implements java.sql.RowId{
	private final String id;

	public RowId2(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	@Override
	public byte[] getBytes() {
		return id.getBytes(StandardCharsets.UTF_8);
	}
}
