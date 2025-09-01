package net.xdob.jdbc.sql;


import net.xdob.ratly.proto.jdbc.ColumnMetaProto;

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
    return name;
  }

  public ColumnMetaProto toProto(){
    return toProto(this);
  }

  public static ColumnInfo from(ColumnMetaProto colMeta) {
    ColumnInfo info = new ColumnInfo(
        colMeta.getName(),
        colMeta.getJdbcType(),
        colMeta.getTypeName(),
        colMeta.getPrecision(),
        colMeta.getScale());
    info.setLabel(colMeta.getLabel());
    return info;
  }

  public static ColumnMetaProto toProto(ColumnInfo info) {
    ColumnMetaProto.Builder builder = ColumnMetaProto.newBuilder()
        .setJdbcType(info.getType())
        .setTypeName(info.getTypeName())
        .setPrecision(info.getPrecision())
        .setScale(info.getScale());
    if(info.getName()!=null){
      builder.setName(info.getName());
    }
    if(info.getLabel()!=null){
      builder.setLabel(info.getLabel());
    }
    return builder.build();
  }
}
