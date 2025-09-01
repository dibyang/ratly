package net.xdob.jdbc.util;

import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.grant.Grant;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.merge.Merge;


import net.sf.jsqlparser.statement.show.ShowTablesStatement;


public class SQLStatementUtil {

	public enum StatementType {
		DDL,      // 数据定义语言
		DML,      // 数据操作语言
		DCL,      // 数据控制语言
		TCL,      // 事务控制语言
		OTHER,
		UNKNOWN   // 未知类型
	}


	/**
	 * 获取SQL语句的大类类型
	 */
	public static StatementType getStatementType(Statement statement) {
		// DDL语句
		if (statement instanceof CreateTable ||
				statement instanceof Alter ||
				statement instanceof Drop ||
				statement instanceof Truncate ||
				statement instanceof CreateIndex ||
				statement instanceof CreateView) {
			return StatementType.DDL;
		}

		// DML语句
		if (statement instanceof Select ||
				statement instanceof Insert ||
				statement instanceof Update ||
				statement instanceof Delete ||
				statement instanceof Merge) {
			return StatementType.DML;
		}

		// DCL语句
		if (statement instanceof Grant ) {
			return StatementType.DCL;
		}

		// TCL语句
		if (statement instanceof Commit
				|| statement instanceof RollbackStatement
				||statement instanceof SavepointStatement
		    || statement instanceof BeginStatement) {
			return StatementType.TCL;
		}

		// 其他语句
		if (statement instanceof SetStatement
				|| statement instanceof ShowTablesStatement
				|| statement instanceof DescribeStatement
				|| statement instanceof ExplainStatement
				|| statement instanceof KillSessionStatement
				|| statement instanceof ShowSessionsStatement) {
			return StatementType.OTHER;
		}

		return StatementType.UNKNOWN;
	}

	/**
	 * 判断是否为DDL语句
	 */
	public static boolean isDDL(Statement statement) {
		return getStatementType(statement) == StatementType.DDL;
	}

	/**
	 * 判断是否为DML语句
	 */
	public static boolean isDML(Statement statement) {
		return getStatementType(statement) == StatementType.DML;
	}

	/**
	 * 判断是否为DCL语句
	 */
	public static boolean isDCL(Statement statement) {
		return getStatementType(statement) == StatementType.DCL;
	}

	/**
	 * 判断是否为TCL语句
	 */
	public static boolean isTCL(Statement statement) {
		return getStatementType(statement) == StatementType.TCL;
	}

	/**
	 * 判断是否为其他类型语句
	 */
	public static boolean isOther(Statement statement) {
		return getStatementType(statement) == StatementType.OTHER;
	}

	/**
	 * 判断语句是否会对数据做修改
	 */
	public static boolean isModification(Statement statement) {
		return statement instanceof Insert ||
				statement instanceof Update ||
				statement instanceof Delete ||
				statement instanceof Merge||
				isDDL( statement);
	}


}
