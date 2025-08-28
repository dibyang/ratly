package net.xdob.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
public interface ConnSupplier {
  Connection getConnection() throws SQLException;
}
