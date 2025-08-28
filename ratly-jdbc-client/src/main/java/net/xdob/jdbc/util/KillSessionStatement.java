package net.xdob.jdbc.util;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;


public class KillSessionStatement implements Statement {

	private Expression sessionId; // 用于存储会话ID，可以是一个数字或更复杂的表达式

	public Expression getSessionId() {
		return sessionId;
	}

	public void setSessionId(Expression sessionId) {
		this.sessionId = sessionId;
	}

	@Override
	public String toString() {
		// 这是最关键的方法之一，用于将 AST 转换回 SQL 字符串
		return "KILL SESSION " + sessionId.toString();
	}

	@Override
	public void accept(StatementVisitor statementVisitor) {

	}
}
