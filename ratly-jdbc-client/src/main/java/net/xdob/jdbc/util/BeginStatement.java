package net.xdob.jdbc.util;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;

public class BeginStatement implements Statement {
	@Override
	public void accept(StatementVisitor statementVisitor) {

	}

	@Override
	public String toString() {
		return "BEGIN";
	}
}
