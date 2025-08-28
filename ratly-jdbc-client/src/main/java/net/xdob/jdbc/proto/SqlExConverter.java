package net.xdob.jdbc.proto;

import net.xdob.ratly.proto.jdbc.SqlEx;

import java.io.IOException;
import java.sql.SQLException;

public class SqlExConverter {
	public static SqlEx toProto(SQLException e) {
		if(e!=null) {
			SqlEx.Builder builder = SqlEx.newBuilder()
					.setMessage(e.getMessage() != null ? e.getMessage() : "")
					.setErrorCode(e.getErrorCode());

			String sqlState = e.getSQLState();
			if (sqlState != null) {
				builder.setSqlState(sqlState);
			}

			// 处理异常链
			SQLException next = e.getNextException();
			if (next != null) {
				builder.addCauses(toProto(next));
			}

			return builder.build();
		}
		return null;
	}

	// 将 protobuf 消息转换回 SQLException（可选）
	public static SQLException fromProto(SqlEx proto) {
		SQLException root = new SQLException(
				proto.getMessage(),
				proto.getSqlState(),
				proto.getErrorCode()
		);

		SQLException current = root;
		for (SqlEx cause : proto.getCausesList()) {
			SQLException nextException = new SQLException(
					cause.getMessage(),
					cause.getSqlState(),
					cause.getErrorCode()
			);
			current.setNextException(nextException);
			current = nextException;
		}

		return root;
	}

}
