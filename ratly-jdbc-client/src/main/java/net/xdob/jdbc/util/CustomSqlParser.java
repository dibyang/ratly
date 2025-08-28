package net.xdob.jdbc.util;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

public class CustomSqlParser {
	public static Statement parse(String sql) throws JSQLParserException {
		String trimmedSql = sql.trim().toUpperCase();

		if(trimmedSql.startsWith("BEGIN")){
			String afterKeyword = sql.substring(sql.indexOf("BEGIN")).trim();
			if(afterKeyword.startsWith("TRANSACTION")){
				return new BeginTransactionStatement();
			}else{
				return new BeginStatement();
			}
		}else if (trimmedSql.startsWith("KILL ")&&trimmedSql.substring("KILL ".length())
				.trim().startsWith("SESSION ")) {
			// 3. 创建我们的自定义语句对象
			KillSessionStatement killSessionStatement = new KillSessionStatement();

			// 4. 提取 "KILL SESSION" 之后的部分，例如 "123"
			// 计算 "KILL SESSION" 之后的起始位置（长度 + 1 个空格）
			String afterKeyword = sql.substring(sql.indexOf(" SESSION ")).trim();

			// 5. 使用 JSqlParser 的表达式解析器来解析剩余部分
			// 这非常强大，因为它可以处理数字、变量、甚至是复杂表达式（如果需要的话）
			Expression sessionIdExpr = CCJSqlParserUtil.parseExpression(afterKeyword);
			killSessionStatement.setSessionId(sessionIdExpr);

			return killSessionStatement;
		}else if (trimmedSql.startsWith("SHOW ")&&trimmedSql.substring("SHOW ".length())
				.trim().startsWith("SESSIONS")) {
			return new ShowSessionsStatement();
		}

		// 6. 如果不是 KILL 语句，则回退到标准解析器
		return CCJSqlParserUtil.parse(sql);
	}
}
