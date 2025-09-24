package net.xdob.ratly.jdbc.sql;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.RowId;
import java.util.Objects;

public class SerialRowId implements RowId, Serializable {
  private final String id;

  public SerialRowId(String id) {
    this.id = id;
  }

  public SerialRowId(RowId rowId) {
    this(rowId.toString());
  }


  @Override
  public byte[] getBytes() {
    return id.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id);
  }

  @Override
  public boolean equals(Object obj) {
    if(obj instanceof RowId) {
      return Objects.equals(id, obj.toString());
    }
    return false;
  }

  @Override
  public String toString() {
    return id;
  }
}
