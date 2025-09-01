package net.xdob.jdbc.util;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;

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
		String processedSql = sql.replaceAll("\\s*\\|\\|\\s*", " || ");
		// 回退到标准解析器
		return CCJSqlParserUtil.parse(processedSql);
	}

	public static void main(String[] args) throws JSQLParserException {
		String sql = "select blocks0_.id as id1_18_, blocks0_.block_size as block_si2_18_, blocks0_.create_time as create_t3_18_, blocks0_.block_id as block_id4_18_, blocks0_.name as name5_18_, blocks0_.need_scheduled_snapshot as need_sch6_18_, blocks0_.nick_name as nick_nam7_18_, blocks0_.path as path8_18_, blocks0_.parent_id as parent_i9_18_, blocks0_.resource_pool_name as resourc10_18_, blocks0_.scheduled_snapshot as schedul11_18_, blocks0_.span_number as span_nu12_18_, blocks0_.span_size as span_si13_18_, blocks0_.update_time as update_14_18_, blocks0_.vol_size as vol_siz15_18_ from store_blocks blocks0_ where (blocks0_.nick_name like ('%'||?||'%')) and (blocks0_.name not like (?||'%')) and (blocks0_.parent_id is null) and blocks0_.resource_pool_name=? order by blocks0_.block_id desc limit ?";
		Statement parse = CustomSqlParser.parse(sql);
		System.out.println(parse);
	}
}
