package net.xdob.jdbc;


import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import net.sf.jsqlparser.statement.select.Select;
import net.xdob.jdbc.util.CustomSqlParser;

public class Test {

	public static void main(String[] args) throws JSQLParserException {
		Select select = (Select)CustomSqlParser.parse("SELECT * FROM B_DB.\"PUBLIC\".BMSQL_ITEM LIMIT :name OFFSET 90000;");
		long start = 0;
		int pageSize = 200;
		Limit limit = select.getLimit();
		if(limit !=null){
			Expression rowCount = limit.getRowCount();
			if(rowCount instanceof LongValue) {
				limit.withRowCount(new LongValue(((LongValue)rowCount).getValue() - start));
			}
			if(rowCount instanceof JdbcParameter) {
				Subtraction subtraction = new Subtraction();
				subtraction.withLeftExpression(rowCount)
								.withRightExpression(new LongValue(start));
				limit.withRowCount(subtraction);
			}
			if(rowCount instanceof JdbcNamedParameter) {
				Subtraction subtraction = new Subtraction();
				subtraction.withLeftExpression(rowCount)
						.withRightExpression(new LongValue(start));
				limit.withRowCount(subtraction);
			}

		}else {
			select.setLimit(new Limit()
					.withRowCount(new LongValue(pageSize + 1)));
		}

		Offset offset = select.getOffset();
		if(offset==null){
			select.setOffset(new Offset()
					.withOffset(new LongValue(start)));
		}else{
			long offsetValue = Long.parseLong(offset.getOffset().toString());
			offset.withOffset(new LongValue(offsetValue + start));
		}

		System.out.println("select.getLimit() = " + select.getLimit().getRowCount());
		System.out.println("select.getOffset() = " + select.getOffset().getOffset());
		System.out.println("select.toString() = " + select.toString());

	}
}
