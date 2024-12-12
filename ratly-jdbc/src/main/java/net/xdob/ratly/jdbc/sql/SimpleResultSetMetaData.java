package net.xdob.ratly.jdbc.sql;

import com.google.common.collect.Lists;
import org.h2.message.DbException;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueToObjectConverter;

import java.io.Serializable;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public class SimpleResultSetMetaData implements ResultSetMetaData, Serializable {
  private final List<SimpleColumnInfo> columns = Lists.newArrayList();

  public SimpleResultSetMetaData() {

  }

  public SimpleResultSetMetaData(ResultSetMetaData metaData) throws SQLException {
    fill(metaData);
  }

  public SimpleResultSetMetaData fill(ResultSetMetaData metaData) throws SQLException {
    int columnCount = metaData.getColumnCount();
    for (int colum = 1; colum <= columnCount; colum++) {
      String name = metaData.getColumnName(colum);
      int sqlType = metaData.getColumnType(colum);
      String sqlTypeName = metaData.getColumnTypeName(colum);
      int precision = metaData.getPrecision(colum);
      int scale = metaData.getScale(colum);
      addColumn(name, sqlType, sqlTypeName, precision, scale);
    }
    return this;
  }

  public void addColumn(String name, int sqlType, int precision, int scale) {
    int valueType = DataType.convertSQLTypeToValueType(sqlType);
    this.addColumn(name, sqlType, Value.getTypeName(valueType), precision, scale);
  }

  /**
   * Adds a column to the result set.
   * All columns must be added before adding rows.
   *
   * @param name        null is replaced with C1, C2,...
   * @param sqlType     the value returned in getColumnType(.)
   * @param sqlTypeName the type name return in getColumnTypeName(.)
   * @param precision   the precision
   * @param scale       the scale
   */
  public void addColumn(String name, int sqlType, String sqlTypeName,
                        int precision, int scale) {
    if (name == null) {
      name = "C" + (columns.size() + 1);
    }
    columns.add(new SimpleColumnInfo(name, sqlType, sqlTypeName, precision, scale));
  }


  private void checkColumnIndex(int columnIndex) throws SQLException {
    if (columnIndex < 1 || columnIndex > columns.size()) {
      throw DbException.getInvalidValueException(
          "columnIndex", columnIndex).getSQLException();
    }
  }

  public SimpleColumnInfo getColumn(int i) throws SQLException {
    checkColumnIndex(i + 1);
    return columns.get(i);
  }

  /**
   * Returns the column count.
   *
   * @return the column count
   */
  @Override
  public int getColumnCount() {
    return columns.size();
  }

  /**
   * Returns 15.
   *
   * @param columnIndex (1,2,...)
   * @return 15
   */
  @Override
  public int getColumnDisplaySize(int columnIndex) {
    return 15;
  }

  /**
   * Returns the SQL type.
   *
   * @param columnIndex (1,2,...)
   * @return the SQL type
   */
  @Override
  public int getColumnType(int columnIndex) throws SQLException {
    return getColumn(columnIndex - 1).type;
  }

  /**
   * Returns the precision.
   *
   * @param columnIndex (1,2,...)
   * @return the precision
   */
  @Override
  public int getPrecision(int columnIndex) throws SQLException {
    return getColumn(columnIndex - 1).precision;
  }

  /**
   * Returns the scale.
   *
   * @param columnIndex (1,2,...)
   * @return the scale
   */
  @Override
  public int getScale(int columnIndex) throws SQLException {
    return getColumn(columnIndex - 1).scale;
  }

  /**
   * Returns ResultSetMetaData.columnNullableUnknown.
   *
   * @param columnIndex (1,2,...)
   * @return columnNullableUnknown
   */
  @Override
  public int isNullable(int columnIndex) {
    return ResultSetMetaData.columnNullableUnknown;
  }

  /**
   * Returns false.
   *
   * @param columnIndex (1,2,...)
   * @return false
   */
  @Override
  public boolean isAutoIncrement(int columnIndex) {
    return false;
  }

  /**
   * Returns true.
   *
   * @param columnIndex (1,2,...)
   * @return true
   */
  @Override
  public boolean isCaseSensitive(int columnIndex) {
    return true;
  }

  /**
   * Returns false.
   *
   * @param columnIndex (1,2,...)
   * @return false
   */
  @Override
  public boolean isCurrency(int columnIndex) {
    return false;
  }

  /**
   * Returns false.
   *
   * @param columnIndex (1,2,...)
   * @return false
   */
  @Override
  public boolean isDefinitelyWritable(int columnIndex) {
    return false;
  }

  /**
   * Returns true.
   *
   * @param columnIndex (1,2,...)
   * @return true
   */
  @Override
  public boolean isReadOnly(int columnIndex) {
    return true;
  }

  /**
   * Returns true.
   *
   * @param columnIndex (1,2,...)
   * @return true
   */
  @Override
  public boolean isSearchable(int columnIndex) {
    return true;
  }

  /**
   * Returns true.
   *
   * @param columnIndex (1,2,...)
   * @return true
   */
  @Override
  public boolean isSigned(int columnIndex) {
    return true;
  }

  /**
   * Returns false.
   *
   * @param columnIndex (1,2,...)
   * @return false
   */
  @Override
  public boolean isWritable(int columnIndex) {
    return false;
  }

  /**
   * Returns empty string.
   *
   * @param columnIndex (1,2,...)
   * @return empty string
   */
  @Override
  public String getCatalogName(int columnIndex) {
    return "";
  }

  /**
   * Returns the Java class name if this column.
   *
   * @param columnIndex (1,2,...)
   * @return the class name
   */
  @Override
  public String getColumnClassName(int columnIndex) throws SQLException {
    int type = DataType.getValueTypeFromResultSet(this, columnIndex);
    return ValueToObjectConverter.getDefaultClass(type, true).getName();
  }

  /**
   * Returns the column label.
   *
   * @param columnIndex (1,2,...)
   * @return the column label
   */
  @Override
  public String getColumnLabel(int columnIndex) throws SQLException {
    return getColumn(columnIndex - 1).name;
  }

  /**
   * Returns the column name.
   *
   * @param columnIndex (1,2,...)
   * @return the column name
   */
  @Override
  public String getColumnName(int columnIndex) throws SQLException {
    return getColumnLabel(columnIndex);
  }

  /**
   * Returns the data type name of a column.
   *
   * @param columnIndex (1,2,...)
   * @return the type name
   */
  @Override
  public String getColumnTypeName(int columnIndex) throws SQLException {
    return getColumn(columnIndex - 1).typeName;
  }

  /**
   * Returns empty string.
   *
   * @param columnIndex (1,2,...)
   * @return empty string
   */
  @Override
  public String getSchemaName(int columnIndex) {
    return "";
  }

  /**
   * Returns empty string.
   *
   * @param columnIndex (1,2,...)
   * @return empty string
   */
  @Override
  public String getTableName(int columnIndex) {
    return "";
  }

  /**
   * Return an object of this class if possible.
   *
   * @param iface the class
   * @return this
   */
  @Override
  @SuppressWarnings("unchecked")
  public <T> T unwrap(Class<T> iface) throws SQLException {
    try {
      if (isWrapperFor(iface)) {
        return (T) this;
      }
      throw DbException.getInvalidValueException("iface", iface);
    } catch (Exception e) {
      throw DbException.toSQLException(e);
    }
  }

  /**
   * Checks if unwrap can return an object of this class.
   *
   * @param iface the class
   * @return whether the interface is assignable from this class
   */
  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface != null && iface.isAssignableFrom(getClass());
  }
}
