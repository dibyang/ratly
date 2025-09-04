package net.xdob.jdbc.sql;

public enum TransactionIsolation {
	NONE,
	SERIALIZABLE,
	REPEATABLE_READ,
	READ_COMMITTED,
	READ_UNCOMMITTED;

	public int getLevel() {
		switch (this) {
			case SERIALIZABLE:
				return java.sql.Connection.TRANSACTION_SERIALIZABLE;
			case REPEATABLE_READ:
				return java.sql.Connection.TRANSACTION_REPEATABLE_READ;
			case READ_COMMITTED:
				return java.sql.Connection.TRANSACTION_READ_COMMITTED;
			case READ_UNCOMMITTED:
				return java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
			default:
				return java.sql.Connection.TRANSACTION_NONE;
		}
	}

	public static TransactionIsolation of(String name) {
		for (TransactionIsolation value : values()) {
			if (value.name().equalsIgnoreCase(name)) {
				return value;
			}
		}
		return NONE;
	}

	public static TransactionIsolation of(int  level) {
		for (TransactionIsolation value : values()) {
			if (value.getLevel()==level) {
				return value;
			}
		}
		return NONE;
	}
}
