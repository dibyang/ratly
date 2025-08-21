package net.xdob.ratly.jdbc.sql;

import net.xdob.ratly.proto.jdbc.SavepointProto;

import java.io.Serializable;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Objects;

public class JdbcSavepoint implements Savepoint, Serializable {
  private final int id;
  private final String name;

  public JdbcSavepoint(int id, String name) {
    this.id = id;
    this.name = name;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    JdbcSavepoint that = (JdbcSavepoint) o;
    return id == that.id && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name);
  }

  @Override
  public int getSavepointId() throws SQLException {
    return id;
  }

  @Override
  public String getSavepointName() throws SQLException {
    return name;
  }

  public SavepointProto toProto() throws SQLException {
    return toProto( this);
  }

  public static JdbcSavepoint of(Savepoint savepoint) throws SQLException {
    if(savepoint instanceof JdbcSavepoint){
      return (JdbcSavepoint) savepoint;
    }
    return new JdbcSavepoint(savepoint.getSavepointId(), savepoint.getSavepointName());
  }

  public static Savepoint from(SavepointProto savepoint) throws SQLException {
    return new JdbcSavepoint(savepoint.getId(), savepoint.getName());
  }

  public static SavepointProto toProto(Savepoint savepoint) throws SQLException {
    return SavepointProto.newBuilder()
        .setId(savepoint.getSavepointId())
        .setName(savepoint.getSavepointName())
        .build();
  }
}
