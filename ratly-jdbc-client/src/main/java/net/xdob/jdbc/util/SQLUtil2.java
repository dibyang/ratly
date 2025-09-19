package net.xdob.jdbc.util;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.util.JdbcConstants;
import net.xdob.jdbc.exception.SQLParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;


public interface SQLUtil2 {
	Logger LOG = LoggerFactory.getLogger(SQLUtil2.class);

	static SQLStatement parse(String sql) throws SQLException {
		try {
			return SQLUtils.parseSingleStatement(sql, JdbcConstants.H2, false);
		} catch (ParserException e) {
			try {
				return SQLUtils.parseSingleStatement(sql, JdbcConstants.MYSQL, false);
			} catch (ParserException ignore) {
			}
			LOG.warn("parse sql error. sql="+sql, e);
			throw new SQLParserException("parse sql error. sql="+sql, e);
		}
	}

	/**
	 * 获取SQL语句的大类类型
	 */
	static SqlType getSqlType(SQLStatement stmt) {
		// DDL语句
		if (
//				stmt instanceof SQLCreateTableStatement ||
//				stmt instanceof SQLAlterTableStatement ||
//				stmt instanceof SQLDropTableStatement ||
//				stmt instanceof SQLCreateViewStatement ||
//				stmt instanceof SQLCreateIndexStatement ||
//				stmt instanceof SQLDropIndexStatement ||
//				stmt instanceof SQLCreateFunctionStatement ||
//				stmt instanceof SQLCreateProcedureStatement ||
//				stmt instanceof SQLDropViewStatement ||
//				stmt instanceof SQLDropFunctionStatement ||
//				stmt instanceof SQLDropProcedureStatement ||
//				stmt instanceof SQLCreateDatabaseStatement ||
//				stmt instanceof SQLDropDatabaseStatement ||
//				stmt instanceof SQLAlterViewStatement ||
//				stmt instanceof SQLAlterIndexStatement||
				stmt instanceof SQLTruncateStatement ||
				stmt instanceof SQLDDLStatement) {
			return SqlType.DDL;
		}

		// DML语句
		if (stmt instanceof SQLInsertStatement
				||stmt instanceof SQLUpdateStatement
				||stmt instanceof SQLDeleteStatement
				||stmt instanceof SQLMergeStatement
				||stmt instanceof SQLCallStatement ) {
			return SqlType.DML;
		}

		// DCL语句
		if (stmt instanceof SQLGrantStatement ||
				stmt instanceof SQLRevokeStatement ||
				stmt instanceof SQLRenameUserStatement) {
			return SqlType.DCL;
		}

		// TCL语句
		if (stmt instanceof SQLCommitStatement ||
				stmt instanceof SQLRollbackStatement ||
				stmt instanceof SQLCommitTransactionStatement ||
				stmt instanceof SQLSavePointStatement ||
				stmt instanceof SQLReleaseSavePointStatement) {
			return SqlType.TCL;
		}

		return SqlType.OTHER;
	}

	/**
	 * 判断语句是否会对数据做修改
	 */
	static boolean isModification(SQLStatement stmt) {
		return getSqlType(stmt).isModification();
	}

	static boolean isCommit(SQLStatement stmt) {
		return stmt instanceof SQLCommitStatement ||
				stmt instanceof SQLCommitTransactionStatement ;
	}

	static boolean isRollback(SQLStatement stmt) {
		return stmt instanceof SQLRollbackStatement;
	}

	public static void main(String[] args) throws SQLException {
		//String sql = "create table store_node_config_info (id varchar(36) not null, client_ip varchar(255), client_status integer, connect_ip varchar(255), first_one boolean, fs varchar(255), full_format boolean, index integer, ips varchar(255), mds boolean, need_format boolean, nic_info clob, start_client_result clob, start_result clob, status integer, template_name varchar(255), template_type varchar(255), total_disk_num integer, use_template_id varchar(255), primary key (id))";
		String sql = "show session";
		SQLStatement stmt = SQLUtil2.parse(sql);
		System.out.println("stmt = " + stmt.getClass());
	}
}
