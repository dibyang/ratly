package net.xdob.jdbc.sql;

import com.google.common.collect.Lists;
import net.xdob.ratly.proto.jdbc.ResultSetProto;
import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;


public class SerialResultSet implements ResultSet, Serializable {
  static Logger LOG = LoggerFactory.getLogger(SerialResultSet.class);

  private final List<SerialRow> rows = Lists.newArrayList();
  private final SerialResultSetMetaData resultSetMetaData = new SerialResultSetMetaData();
  private int rowId = -1;
  private transient SerialRow currentRow;
  private transient boolean wasNull;
  private transient boolean autoClose = true;
  private transient boolean closed = true;

  public SerialResultSet(ResultSetMetaData resultSetMetaData) throws SQLException {
    this.resultSetMetaData.fill(resultSetMetaData);
  }

  public SerialResultSet(ResultSet rs) throws SQLException {
    resultSetMetaData.fill(rs.getMetaData());
    while (rs.next()) {
      SerialRow row = new SerialRow(resultSetMetaData.getColumnCount());
      for (int col = 1; col <= resultSetMetaData.getColumnCount(); col++) {
        row.setValue(col - 1, rs.getObject(col));
      }
      rows.add(row);
      //LOG.info("add row={}", row);
    }
  }

  public void resetResult() {
    if (!rows.isEmpty()) {
      rowId = -1;
    }
    currentRow = null;
  }

  public SerialResultSet addRows(SerialRow... rows) {
    return addRows(Arrays.asList(rows));
  }

  public SerialResultSet addRows(Collection<SerialRow> rows) {
    this.rows.addAll(rows);
    return this;
  }

  /**
   * Returns ResultSet.CONCUR_READ_ONLY.
   *
   * @return CONCUR_READ_ONLY
   */
  @Override
  public int getConcurrency() {
    return ResultSet.CONCUR_READ_ONLY;
  }

  /**
   * Returns ResultSet.FETCH_FORWARD.
   *
   * @return FETCH_FORWARD
   */
  @Override
  public int getFetchDirection() {
    return ResultSet.FETCH_FORWARD;
  }

  /**
   * Returns 0.
   *
   * @return 0
   */
  @Override
  public int getFetchSize() {
    return 0;
  }

  /**
   * Returns the row number (1, 2,...) or 0 for no row.
   *
   * @return 0
   */
  @Override
  public int getRow() {
    return currentRow == null ? 0 : (int)(rowId + 1);
  }

  /**
   * Returns the result set type. This is ResultSet.TYPE_FORWARD_ONLY for
   * auto-close result sets, and ResultSet.TYPE_SCROLL_INSENSITIVE for others.
   *
   * @return TYPE_FORWARD_ONLY or TYPE_SCROLL_INSENSITIVE
   */
  @Override
  public int getType() {
    if (autoClose) {
      return ResultSet.TYPE_FORWARD_ONLY;
    }
    return ResultSet.TYPE_SCROLL_INSENSITIVE;
  }

  /**
   * Closes the result set and releases the resources.
   */
  @Override
  public void close() {
    closed = true;
    currentRow = null;
    rowId = -1;

  }

  /**
   * Moves the cursor to the next row of the result set.
   *
   * @return true if successful, false if there are no more rows
   */
  @Override
  public boolean next() throws SQLException {
    if (rowId < rows.size()) {
      rowId++;
      if (rowId < rows.size()) {
        currentRow = rows.get(rowId);
        return true;
      }
      currentRow = null;
    }
    if (autoClose) {
      close();
    }
    return false;
  }

  /**
   * Moves the current position to before the first row, that means the result
   * set is reset.
   */
  @Override
  public void beforeFirst() throws SQLException {
    if (autoClose) {
      throw DbException.getJdbcSQLException(ErrorCode.RESULT_SET_NOT_SCROLLABLE);
    }
    rowId = -1;

  }

  /**
   * Returns whether the last column accessed was null.
   *
   * @return true if the last column accessed was null
   */
  @Override
  public boolean wasNull() {
    return wasNull;
  }

  /**
   * Searches for a specific column in the result set. A case-insensitive
   * search is made.
   *
   * @param columnLabel the column label
   * @return the column index (1,2,...)
   * @throws SQLException if the column is not found or if the result set is
   *                      closed
   */
  @Override
  public int findColumn(String columnLabel) throws SQLException {
    if (columnLabel != null) {
      for (int i = 0, size = resultSetMetaData.getColumnCount(); i < size; i++) {
        ColumnInfo column = resultSetMetaData.getColumn(i);
        if (columnLabel.equalsIgnoreCase(column.getLabel())
            ||columnLabel.equalsIgnoreCase(column.getName())) {
          return i + 1;
        }
      }
    }
    throw DbException.getJdbcSQLException(ErrorCode.COLUMN_NOT_FOUND_1, columnLabel);
  }

  /**
   * Returns a reference to itself.
   *
   * @return this
   */
  @Override
  public ResultSetMetaData getMetaData() {
    return resultSetMetaData;
  }

  /**
   * Returns null.
   *
   * @return null
   */
  @Override
  public SQLWarning getWarnings() {
    return null;
  }

  /**
   * Returns null.
   *
   * @return null
   */
  @Override
  public Statement getStatement() {
    return null;
  }

  /**
   * INTERNAL
   */
  @Override
  public void clearWarnings() {
    // nothing to do
  }

  // ---- get ---------------------------------------------

  /**
   * Returns the value as a java.sql.Array.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @Override
  public Array getArray(int columnIndex) throws SQLException {
    Object[] o = (Object[]) get(columnIndex);
    return o == null ? null : new SimpleArray(o);
  }

  /**
   * Returns the value as a java.sql.Array.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @Override
  public Array getArray(String columnLabel) throws SQLException {
    return getArray(findColumn(columnLabel));
  }

  /**
   * INTERNAL
   */
  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * Returns the value as a java.math.BigDecimal.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    Object o = get(columnIndex);
    if (o != null && !(o instanceof BigDecimal)) {
      o = new BigDecimal(o.toString());
    }
    return (BigDecimal) o;
  }

  /**
   * Returns the value as a java.math.BigDecimal.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return getBigDecimal(findColumn(columnLabel));
  }

  /**
   * @deprecated INTERNAL
   */
  @Deprecated
  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale)
      throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * @deprecated INTERNAL
   */
  @Deprecated
  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale)
      throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * Returns the value as a java.io.InputStream.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    return asInputStream(get(columnIndex));
  }

  private static InputStream asInputStream(Object o) throws SQLException {
    if (o == null) {
      return null;
    } else if (o instanceof Blob) {
      return ((Blob) o).getBinaryStream();
    }
    return (InputStream) o;
  }

  /**
   * Returns the value as a java.io.InputStream.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return getBinaryStream(findColumn(columnLabel));
  }

  /**
   * Returns the value as a java.sql.Blob.
   * This is only supported if the
   * result set was created using a Blob object.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    Object o = get(columnIndex);
    if(o instanceof byte[]){
      o = new SerialBlob((byte[])o);
    }
    return (Blob) o;
  }

  /**
   * Returns the value as a java.sql.Blob.
   * This is only supported if the
   * result set was created using a Blob object.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    return getBlob(findColumn(columnLabel));
  }

  /**
   * Returns the value as a boolean.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    Object o = get(columnIndex);
    if (o == null) {
      return false;
    }
    if (o instanceof Boolean) {
      return (Boolean) o;
    }
    if (o instanceof Number) {
      Number n = (Number) o;
      if (n instanceof Double || n instanceof Float) {
        return n.doubleValue() != 0;
      }
      if (n instanceof BigDecimal) {
        return ((BigDecimal) n).signum() != 0;
      }
      if (n instanceof BigInteger) {
        return ((BigInteger) n).signum() != 0;
      }
      return n.longValue() != 0;
    }
    return Utils.parseBoolean(o.toString(), false, true);
  }

  /**
   * Returns the value as a boolean.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return getBoolean(findColumn(columnLabel));
  }

  /**
   * Returns the value as a byte.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @Override
  public byte getByte(int columnIndex) throws SQLException {
    Object o = get(columnIndex);
    if (o != null && !(o instanceof Number)) {
      o = Byte.decode(o.toString());
    }
    return o == null ? 0 : ((Number) o).byteValue();
  }

  /**
   * Returns the value as a byte.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return getByte(findColumn(columnLabel));
  }

  /**
   * Returns the value as a byte array.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    Object o = get(columnIndex);
    if (o == null || o instanceof byte[]) {
      return (byte[]) o;
    }
    if (o instanceof UUID) {
      return Bits.uuidToBytes((UUID) o);
    }
    return JdbcUtils.serialize(o, null);
  }

  /**
   * Returns the value as a byte array.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return getBytes(findColumn(columnLabel));
  }

  /**
   * Returns the value as a java.io.Reader.
   * This is only supported if the
   * result set was created using a Clob or Reader object.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    return asReader(get(columnIndex));
  }

  private static Reader asReader(Object o) throws SQLException {
    if (o == null) {
      return null;
    } else if (o instanceof Clob) {
      return ((Clob) o).getCharacterStream();
    }
    return (Reader) o;
  }

  /**
   * Returns the value as a java.io.Reader.
   * This is only supported if the
   * result set was created using a Clob or Reader object.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    return getCharacterStream(findColumn(columnLabel));
  }

  /**
   * Returns the value as a java.sql.Clob.
   * This is only supported if the
   * result set was created using a Clob object.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    Object o = get(columnIndex);
    if(o instanceof String){
      o = new SerialClob(((String) o).toCharArray());
    }
    return (Clob) o;
  }

  /**
   * Returns the value as a java.sql.Clob.
   * This is only supported if the
   * result set was created using a Clob object.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    return getClob(findColumn(columnLabel));
  }

  /**
   * Returns the value as a java.sql.Date.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @Override
  public Date getDate(int columnIndex) throws SQLException {
    return (Date) get(columnIndex);
  }

  /**
   * Returns the value as a java.sql.Date.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return getDate(findColumn(columnLabel));
  }

  /**
   * INTERNAL
   */
  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * Returns the value as a double.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @Override
  public double getDouble(int columnIndex) throws SQLException {
    Object o = get(columnIndex);
    if (o != null && !(o instanceof Number)) {
      return Double.parseDouble(o.toString());
    }
    return o == null ? 0 : ((Number) o).doubleValue();
  }

  /**
   * Returns the value as a double.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return getDouble(findColumn(columnLabel));
  }

  /**
   * Returns the value as a float.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @Override
  public float getFloat(int columnIndex) throws SQLException {
    Object o = get(columnIndex);
    if (o != null && !(o instanceof Number)) {
      return Float.parseFloat(o.toString());
    }
    return o == null ? 0 : ((Number) o).floatValue();
  }

  /**
   * Returns the value as a float.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return getFloat(findColumn(columnLabel));
  }

  /**
   * Returns the value as an int.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @Override
  public int getInt(int columnIndex) throws SQLException {
    Object o = get(columnIndex);
    if (o != null && !(o instanceof Number)) {
      o = Integer.decode(o.toString());
    }
    return o == null ? 0 : ((Number) o).intValue();
  }

  /**
   * Returns the value as an int.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @Override
  public int getInt(String columnLabel) throws SQLException {
    return getInt(findColumn(columnLabel));
  }

  /**
   * Returns the value as a long.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @Override
  public long getLong(int columnIndex) throws SQLException {
    Object o = get(columnIndex);
    if (o != null && !(o instanceof Number)) {
      o = Long.decode(o.toString());
    }
    return o == null ? 0 : ((Number) o).longValue();
  }

  /**
   * Returns the value as a long.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @Override
  public long getLong(String columnLabel) throws SQLException {
    return getLong(findColumn(columnLabel));
  }

  /**
   * INTERNAL
   */
  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public String getNString(int columnIndex) throws SQLException {
    return getString(columnIndex);
  }

  /**
   * INTERNAL
   */
  @Override
  public String getNString(String columnLabel) throws SQLException {
    return getString(columnLabel);
  }

  /**
   * Returns the value as an Object.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return get(columnIndex);
  }

  /**
   * Returns the value as an Object.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return getObject(findColumn(columnLabel));
  }

  /**
   * Returns the value as an Object of the specified type.
   *
   * @param columnIndex the column index (1, 2, ...)
   * @param type        the class of the returned value
   * @return the value
   */
  @SuppressWarnings("unchecked")
  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    if (get(columnIndex) == null) {
      return null;
    }
    if (type == BigDecimal.class) {
      return (T) getBigDecimal(columnIndex);
    } else if (type == BigInteger.class) {
      return (T) getBigDecimal(columnIndex).toBigInteger();
    } else if (type == String.class) {
      return (T) getString(columnIndex);
    } else if (type == Boolean.class) {
      return (T) (Boolean) getBoolean(columnIndex);
    } else if (type == Byte.class) {
      return (T) (Byte) getByte(columnIndex);
    } else if (type == Short.class) {
      return (T) (Short) getShort(columnIndex);
    } else if (type == Integer.class) {
      return (T) (Integer) getInt(columnIndex);
    } else if (type == Long.class) {
      return (T) (Long) getLong(columnIndex);
    } else if (type == Float.class) {
      return (T) (Float) getFloat(columnIndex);
    } else if (type == Double.class) {
      return (T) (Double) getDouble(columnIndex);
    } else if (type == Date.class) {
      return (T) getDate(columnIndex);
    } else if (type == Time.class) {
      return (T) getTime(columnIndex);
    } else if (type == Timestamp.class) {
      return (T) getTimestamp(columnIndex);
    } else if (type == UUID.class) {
      return (T) getObject(columnIndex);
    } else if (type == byte[].class) {
      return (T) getBytes(columnIndex);
    } else if (type == Array.class) {
      return (T) getArray(columnIndex);
    } else if (type == Blob.class) {
      return (T) getBlob(columnIndex);
    } else if (type == Clob.class) {
      return (T) getClob(columnIndex);
    } else {
      throw getUnsupportedException();
    }
  }

  /**
   * Returns the value as an Object of the specified type.
   *
   * @param columnName the column name
   * @param type       the class of the returned value
   * @return the value
   */
  @Override
  public <T> T getObject(String columnName, Class<T> type) throws SQLException {
    return getObject(findColumn(columnName), type);
  }

  /**
   * INTERNAL
   */
  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map)
      throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map)
      throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * Returns the value as a short.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @Override
  public short getShort(int columnIndex) throws SQLException {
    Object o = get(columnIndex);
    if (o != null && !(o instanceof Number)) {
      o = Short.decode(o.toString());
    }
    return o == null ? 0 : ((Number) o).shortValue();
  }

  /**
   * Returns the value as a short.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @Override
  public short getShort(String columnLabel) throws SQLException {
    return getShort(findColumn(columnLabel));
  }

  /**
   * INTERNAL
   */
  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * Returns the value as a String.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @Override
  public String getString(int columnIndex) throws SQLException {
    Object o = get(columnIndex);
    if (o == null) {
      return null;
    }
    if (o instanceof Clob) {
      Clob c = (Clob) o;
      return c.getSubString(1, MathUtils.convertLongToInt(c.length()));
    }
    return o.toString();
  }

  /**
   * Returns the value as a String.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @Override
  public String getString(String columnLabel) throws SQLException {
    return getString(findColumn(columnLabel));
  }

  /**
   * Returns the value as a java.sql.Time.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return (Time) get(columnIndex);
  }

  /**
   * Returns the value as a java.sql.Time.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return getTime(findColumn(columnLabel));
  }

  /**
   * INTERNAL
   */
  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * Returns the value as a java.sql.Timestamp.
   *
   * @param columnIndex (1,2,...)
   * @return the value
   */
  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return (Timestamp) get(columnIndex);
  }

  /**
   * Returns the value as a java.sql.Timestamp.
   *
   * @param columnLabel the column label
   * @return the value
   */
  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return getTimestamp(findColumn(columnLabel));
  }

  /**
   * INTERNAL
   */
  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal)
      throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal)
      throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * @deprecated INTERNAL
   */
  @Deprecated
  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * @deprecated INTERNAL
   */
  @Deprecated
  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public URL getURL(int columnIndex) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public URL getURL(String columnLabel) throws SQLException {
    throw getUnsupportedException();
  }

  // ---- update ---------------------------------------------

  /**
   * INTERNAL
   */
  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateAsciiStream(int columnIndex, InputStream x)
      throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateAsciiStream(String columnLabel, InputStream x)
      throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length)
      throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length)
      throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x)
      throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x)
      throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateBinaryStream(int columnIndex, InputStream x)
      throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateBinaryStream(String columnLabel, InputStream x)
      throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length)
      throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length)
      throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateBlob(int columnIndex, InputStream x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateBlob(String columnLabel, InputStream x)
      throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateBlob(int columnIndex, InputStream x, long length)
      throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateBlob(String columnLabel, InputStream x, long length)
      throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateBoolean(String columnLabel, boolean x)
      throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateCharacterStream(int columnIndex, Reader x)
      throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateCharacterStream(String columnLabel, Reader x)
      throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length)
      throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateCharacterStream(String columnLabel, Reader x, int length)
      throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateCharacterStream(String columnLabel, Reader x, long length)
      throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateClob(int columnIndex, Reader x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateClob(String columnLabel, Reader x) throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateClob(int columnIndex, Reader x, long length)
      throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateClob(String columnLabel, Reader x, long length)
      throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateNCharacterStream(int columnIndex, Reader x)
      throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateNCharacterStream(String columnLabel, Reader x)
      throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateNCharacterStream(String columnLabel, Reader x, long length)
      throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateNClob(int columnIndex, NClob x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateNClob(String columnLabel, NClob x) throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateNClob(int columnIndex, Reader x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateNClob(String columnLabel, Reader x) throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateNClob(int columnIndex, Reader x, long length)
      throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateNClob(String columnLabel, Reader x, long length)
      throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateNString(int columnIndex, String x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateNString(String columnLabel, String x) throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateNull(int columnIndex) throws SQLException {
    update(columnIndex, null);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateNull(String columnLabel) throws SQLException {
    update(columnLabel, null);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateObject(int columnIndex, Object x, int scale)
      throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateObject(String columnLabel, Object x, int scale)
      throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateSQLXML(int columnIndex, SQLXML x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateSQLXML(String columnLabel, SQLXML x) throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    update(columnLabel, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateTimestamp(int columnIndex, Timestamp x)
      throws SQLException {
    update(columnIndex, x);
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateTimestamp(String columnLabel, Timestamp x)
      throws SQLException {
    update(columnLabel, x);
  }


  /**
   * INTERNAL
   */
  @Override
  public void afterLast() throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public void cancelRowUpdates() throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public void deleteRow() throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public void insertRow() throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public void moveToCurrentRow() throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public void moveToInsertRow() throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public void refreshRow() throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public void updateRow() throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public boolean first() throws SQLException {
    if (this.rowId >= 0) {
      this.resetResult();
    }
    return this.next();
  }

  /**
   * INTERNAL
   */
  @Override
  public boolean isAfterLast() throws SQLException {
    return rowId >= rows.size();
  }

  /**
   * INTERNAL
   */
  @Override
  public boolean isBeforeFirst() throws SQLException {
    return this.rowId < 0 && !rows.isEmpty();
  }

  /**
   * INTERNAL
   */
  @Override
  public boolean isFirst() throws SQLException {
    return this.rowId == 0 && !rows.isEmpty();
  }

  /**
   * INTERNAL
   */
  @Override
  public boolean isLast() throws SQLException {
    return rowId >= rows.size();
  }

  /**
   * INTERNAL
   */
  @Override
  public boolean last() throws SQLException {
    if (this.isAfterLast()) {
      this.resetResult();
    }
    while(this.next()) {
      //
    }
    return currentRow!=null;
  }

  /**
   * INTERNAL
   */
  @Override
  public boolean previous() throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public boolean rowDeleted() throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public boolean rowInserted() throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public boolean rowUpdated() throws SQLException {
    return true;
  }

  /**
   * INTERNAL
   */
  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public void setFetchSize(int rows) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public boolean absolute(int row) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public boolean relative(int offset) throws SQLException {
    throw getUnsupportedException();
  }

  /**
   * INTERNAL
   */
  @Override
  public String getCursorName() throws SQLException {
    throw getUnsupportedException();
  }

  // --- private -----------------------------

  private void update(int columnIndex, Object obj) throws SQLException {
    checkClosed();
    checkColumnIndex(columnIndex);
    this.currentRow.setValue(columnIndex - 1, obj);
  }

  private void update(String columnLabel, Object obj) throws SQLException {
    this.currentRow.setValue(findColumn(columnLabel) - 1, obj);
  }

  /**
   * INTERNAL
   */
  static SQLException getUnsupportedException() {
    return DbException.getJdbcSQLException(ErrorCode.FEATURE_NOT_SUPPORTED_1);
  }

  private void checkClosed() throws SQLException {
    if (closed) {
      throw DbException.getJdbcSQLException(ErrorCode.OBJECT_CLOSED);
    }
  }

  private void checkColumnIndex(int columnIndex) throws SQLException {
    if (columnIndex < 1 || columnIndex > resultSetMetaData.getColumnCount()) {
      throw DbException.getInvalidValueException(
          "columnIndex", columnIndex).getSQLException();
    }
  }

  private Object get(int columnIndex) throws SQLException {
    if (currentRow == null) {
      throw DbException.getJdbcSQLException(ErrorCode.NO_DATA_AVAILABLE);
    }
    checkColumnIndex(columnIndex);
    columnIndex--;
    Object o = columnIndex < currentRow.getColumns() ?
        currentRow.getValue(columnIndex) : null;
    wasNull = o == null;
    return o;
  }


  /**
   * Returns the current result set holdability.
   *
   * @return the holdability
   */
  @Override
  public int getHoldability() {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  /**
   * Returns whether this result set has been closed.
   *
   * @return true if the result set was closed
   */
  @Override
  public boolean isClosed() {
    return closed;
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

  /**
   * Set the auto-close behavior. If enabled (the default), the result set is
   * closed after reading the last row.
   *
   * @param autoClose the new value
   */
  public void setAutoClose(boolean autoClose) {
    this.autoClose = autoClose;
  }

  /**
   * Get the current auto-close behavior.
   *
   * @return the auto-close value
   */
  public boolean getAutoClose() {
    return autoClose;
  }

  /**
   * A simple array implementation,
   * backed by an object array
   */
  public static class SimpleArray implements Array {

    private final Object[] value;

    SimpleArray(Object[] value) {
      this.value = value;
    }

    /**
     * Get the object array.
     *
     * @return the object array
     */
    @Override
    public Object getArray() {
      return value;
    }

    /**
     * INTERNAL
     */
    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
      throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public Object getArray(long index, int count) throws SQLException {
      throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map)
        throws SQLException {
      throw getUnsupportedException();
    }

    /**
     * Get the base type of this array.
     *
     * @return Types.NULL
     */
    @Override
    public int getBaseType() {
      return Types.NULL;
    }

    /**
     * Get the base type name of this array.
     *
     * @return "NULL"
     */
    @Override
    public String getBaseTypeName() {
      return "NULL";
    }

    /**
     * INTERNAL
     */
    @Override
    public ResultSet getResultSet() throws SQLException {
      throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map)
        throws SQLException {
      throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public ResultSet getResultSet(long index, int count)
        throws SQLException {
      throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public ResultSet getResultSet(long index, int count,
                                  Map<String, Class<?>> map) throws SQLException {
      throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void free() {
      // nothing to do
    }

  }

  @Override
  public String toString() {
    return "{" +
        "rowId=" + rowId +
        ", resultSetMetaData=" + resultSetMetaData +
        ", rows=" + rows +
        '}';
  }

  public ResultSetProto toProto() throws SQLException {
    return toProto(this);
  }

  public static SerialResultSet from(ResultSetProto rs) throws SQLException {
    SerialResultSet serialResultSet = new SerialResultSet(SerialResultSetMetaData.from(rs.getColumnsList()));
    for (ResultSetProto.RowProto row : rs.getRowsList()) {
      serialResultSet.rows.add(SerialRow.from(row));
    }
    return serialResultSet;
  }

  public static ResultSetProto toProto(ResultSet rs) throws SQLException {
    ResultSetProto.Builder builder = ResultSetProto.newBuilder();
    SerialResultSet serialResultSet = new SerialResultSet(rs);

    builder.addAllColumns(serialResultSet.resultSetMetaData.toProto());
    for (SerialRow row : serialResultSet.rows) {
      builder.addRows(SerialRow.toProto(row));
    }
    return builder.build();
  }

  public static void main(String[] args) throws SQLException {
    SerialResultSetMetaData resultSetMetaData = new SerialResultSetMetaData();
    resultSetMetaData.addColumn("val1", JDBCType.INTEGER.getVendorTypeNumber(), 10, 0);
    resultSetMetaData.addColumn("name", JDBCType.VARCHAR.getVendorTypeNumber(), 0, 0);
    resultSetMetaData.addColumn("val2", JDBCType.INTEGER.getVendorTypeNumber(), 10, 0);
    SerialResultSet resultSet = new SerialResultSet(resultSetMetaData);
    resultSet.addRows(new SerialRow(3).setValue(0,1).setValue(1,"ok").setValue(2,3));
    resultSet.next();
    System.out.println(resultSet.getInt(1));
    System.out.println(resultSet.getInt(3));
  }
}
