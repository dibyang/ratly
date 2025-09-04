package net.xdob.jdbc;

import net.xdob.ratly.jdbc.Driver;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Test {

	public static void main(String[] args) {
		String url = "jdbc:ratly:13.13.163.1$n1@13.13.14.163,n2@13.13.14.164/fspool_db?port=7800";
		Driver.load();
		try(Connection connection = DriverManager.getConnection(url, "remote", "hhrhl2016");
				Statement statement = connection.createStatement()){
			ResultSet resultSet = statement.executeQuery("select * from utility_user;");
//			ResultSetMetaData metaData = resultSet.getMetaData();
//			List<String> columns = new ArrayList<>();
//			for (int i = 1; i <= metaData.getColumnCount(); i++) {
//				columns.add(metaData.getColumnName(i));
//			}
			while (resultSet.next()){
				for (int i =1; i< 5;i++) {
					System.out.print(resultSet.getObject(i) + "\t");
				}
				System.out.println("");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
}
