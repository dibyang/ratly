package net.xdob.jdbc.util;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;

public class ShowSessionStatement implements Statement {

	@Override
	public String toString() {
		return "SHOW SESSION";
	}

	@Override
	public void accept(StatementVisitor statementVisitor) {

	}
}
