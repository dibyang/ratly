package net.xdob.ratly.jdbc.sql;

import java.io.Serializable;

public final class SimpleColumnInfo implements Serializable {
  /**
   * Name of the column.
   */
  public final String name;

  /**
   * Type of the column, see {@link java.sql.Types}.
   */
  public final int type;

  /**
   * Type name of the column.
   */
  public final String typeName;

  /**
   * Precision of the column
   */
  public final int precision;

  /**
   * Scale of the column.
   */
  public final int scale;

  /**
   * Creates metadata.
   *
   * @param name
   *            name of the column
   * @param type
   *            type of the column, see {@link java.sql.Types}
   * @param typeName
   *            type name of the column
   * @param precision
   *            precision of the column
   * @param scale
   *            scale of the column
   */
  public SimpleColumnInfo(String name, int type, String typeName, int precision, int scale) {
    this.name = name;
    this.type = type;
    this.typeName = typeName;
    this.precision = precision;
    this.scale = scale;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    SimpleColumnInfo other = (SimpleColumnInfo) obj;
    return name.equals(other.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }
}
