package net.xdob.jdbc.util;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;

public class ShowSessionsStatement implements Statement {

	@Override
	public String toString() {
		return "SHOW SESSIONS";
	}

	@Override
	public void accept(StatementVisitor statementVisitor) {

	}
}
