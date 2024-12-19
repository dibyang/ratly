package net.xdob.ratly.jdbc.sql;

import java.io.Serializable;

public final class ColumnInfo implements Serializable {
  /**
   * Name of the column.
   */
  private final String name;

  private String label;

  /**
   * Type of the column, see {@link java.sql.Types}.
   */
  private final int type;

  /**
   * Type name of the column.
   */
  private final String typeName;

  /**
   * Precision of the column
   */
  private final int precision;

  /**
   * Scale of the column.
   */
  private final int scale;

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
  public ColumnInfo(String name, int type, String typeName, int precision, int scale) {
    this.name = name;
    this.type = type;
    this.typeName = typeName;
    this.precision = precision;
    this.scale = scale;
  }

  public String getName() {
    return name;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public int getType() {
    return type;
  }

  public String getTypeName() {
    return typeName;
  }

  public int getPrecision() {
    return precision;
  }

  public int getScale() {
    return scale;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ColumnInfo other = (ColumnInfo) obj;
    return name.equals(other.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public String toString() {
    return "{" +
        "name='" + name + '\'' +
        ", label='" + label + '\'' +
        ", type=" + type +
        ", typeName='" + typeName + '\'' +
        ", precision=" + precision +
        ", scale=" + scale +
        '}';
  }
}
