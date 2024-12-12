package net.xdob.ratly.examples.jdbc;

import net.xdob.ratly.proto.jdbc.SQLExceptionProto;
import net.xdob.ratly.fasts.serialization.FSTConfiguration;

import java.sql.SQLException;

public abstract class SQLClient extends Client {
  protected final FSTConfiguration fst = FSTConfiguration.createDefaultConfiguration();

  protected SQLException getSQLException(SQLExceptionProto ex) {
    SQLException e = new SQLException(ex.getReason(), ex.getState(), ex.getErrorCode());
    e.setStackTrace((StackTraceElement[]) fst.asObject(ex.getStacktrace().toByteArray()));
    return e;
  }

}
