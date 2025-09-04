package net.xdob.jdbc.sql;

import com.google.common.collect.Maps;
import net.xdob.ratly.proto.jdbc.*;
import net.xdob.ratly.proto.sm.WrapReplyProto;
import net.xdob.ratly.proto.sm.WrapRequestProto;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.Value;
import net.xdob.ratly.util.Proto2Util;
import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.Bits;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;
import java.util.UUID;

public class RemoteResultSetInvocationHandler implements InvocationHandler {

  static Logger LOG = LoggerFactory.getLogger(RemoteResultSetInvocationHandler.class);
  private final SqlClient client;
	private final String uuid;
	private volatile int row = -1;
	private transient SerialRow currentRow;
	private transient boolean wasNull;

	private final SerialResultSetMetaData resultSetMetaData = new SerialResultSetMetaData();

	private final Map<Method, Method> localMethods = Maps.newHashMap();
	private final SqlRequestProto.Builder sqlBuilder;

  public RemoteResultSetInvocationHandler(SqlRequestProto.Builder sqlBuilder, SqlClient client, String uuid, ResultSetMetaData resultMetaData ) throws SQLException {
    this.client = client;
		this.uuid = uuid;
		this.sqlBuilder = sqlBuilder;
		this.resultSetMetaData.fill(resultMetaData);
	}


  public Object invoke(Method method, Object... args) throws SQLException {
		ResultSetRequestProto.Builder request = ResultSetRequestProto.newBuilder();
    request.setUid(uuid).setMethod(method.getName());
    for (Class<?> parameterType : method.getParameterTypes()) {
      request.addParametersTypes(parameterType.getName());
    }
    if(args!=null) {
      for (Object arg : args) {
        request.addArgs(Value.toValueProto(arg));
      }
    }
    request.setRow(row);
		request.setSqlRequest(sqlBuilder);
    JdbcRequestProto.Builder builder = JdbcRequestProto.newBuilder();
    builder.setDb(client.getCi().getDb())
        .setSessionId(client.getConnection().getSession())
        .setTimeoutSeconds(3)
				.setResultSetRequest(request);
		RemoteResultSetProto resultSet = sendJdbcRequest(builder.build());
		row = resultSet.getRow();
		if(resultSet.hasCurRow()){
			currentRow = SerialRow.from(resultSet.getCurRow());
		}else{
			currentRow = null;
		}
		if(resultSet.hasValue()) {
			return Value.toJavaObject(resultSet.getValue());
		}else if(resultSet.getColumnsCount()>0){
			return SerialResultSetMetaData.from(resultSet.getColumnsList());
		}
		return null;
  }


  protected RemoteResultSetProto sendJdbcRequest(JdbcRequestProto queryRequest) throws SQLException {
    JdbcResponseProto response = sendReadOnly(queryRequest);
		//Printer4Proto.printJson(response, s->LOG.info("response: {}", s));
		return response.getRemoteResultSet();
  }

  protected JdbcResponseProto sendReadOnly(JdbcRequestProto request) throws SQLException {
		client.getConnection().checkClose();
		try {
      WrapRequestProto wrap = WrapRequestProto.newBuilder()
          .setType(JdbcConnection.DB)
          .setJdbcRequest(request)
          .build();
      RaftClientReply reply =
          client.getClient().io().sendReadOnly(Message.valueOf(wrap));
      WrapReplyProto replyProto = WrapReplyProto.parseFrom(reply.getMessage().getContent());
      JdbcResponseProto response = replyProto.getJdbcResponse();
      if(response.hasEx()){
        throw Proto2Util.toSQLException(response.getEx());
      }
      return response;
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    if(!localMethods.containsKey(method)) {
      try{
        Method m = this.getClass().getMethod(method.getName(), method.getParameterTypes());
        localMethods.put(method, m);
      }catch(NoSuchMethodException e) {
        localMethods.put(method, null);
      }
    }
    Method m = localMethods.get(method);
    if(m!=null){
      return m.invoke(this, args);
    }else{
      return invoke(method, args);
    }
  }

	public int getRow() {
		return row;
	}

	public boolean wasNull() {
		return wasNull;
	}

	public ResultSetMetaData getMetaData() {
		return resultSetMetaData;
	}

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
	 * Returns the value as a java.sql.Array.
	 *
	 * @param columnIndex (1,2,...)
	 * @return the value
	 */
	public Array getArray(int columnIndex) throws SQLException {
		Object[] o = (Object[]) get(columnIndex);
		return o == null ? null : new SerialResultSet.SimpleArray(o);
	}

	/**
	 * Returns the value as a java.sql.Array.
	 *
	 * @param columnLabel the column label
	 * @return the value
	 */
	public Array getArray(String columnLabel) throws SQLException {
		return getArray(findColumn(columnLabel));
	}

	/**
	 * INTERNAL
	 */
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * INTERNAL
	 */
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * Returns the value as a java.math.BigDecimal.
	 *
	 * @param columnIndex (1,2,...)
	 * @return the value
	 */
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
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return getBigDecimal(findColumn(columnLabel));
	}

	/**
	 * @deprecated INTERNAL
	 */
	@Deprecated
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * @deprecated INTERNAL
	 */
	@Deprecated
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
	public Blob getBlob(String columnLabel) throws SQLException {
		return getBlob(findColumn(columnLabel));
	}

	/**
	 * Returns the value as a boolean.
	 *
	 * @param columnIndex (1,2,...)
	 * @return the value
	 */
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
	public boolean getBoolean(String columnLabel) throws SQLException {
		return getBoolean(findColumn(columnLabel));
	}

	/**
	 * Returns the value as a byte.
	 *
	 * @param columnIndex (1,2,...)
	 * @return the value
	 */
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
	public byte getByte(String columnLabel) throws SQLException {
		return getByte(findColumn(columnLabel));
	}

	/**
	 * Returns the value as a byte array.
	 *
	 * @param columnIndex (1,2,...)
	 * @return the value
	 */
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
	public Clob getClob(String columnLabel) throws SQLException {
		return getClob(findColumn(columnLabel));
	}

	/**
	 * Returns the value as a java.sql.Date.
	 *
	 * @param columnIndex (1,2,...)
	 * @return the value
	 */
	public Date getDate(int columnIndex) throws SQLException {
		return (Date) get(columnIndex);
	}

	/**
	 * Returns the value as a java.sql.Date.
	 *
	 * @param columnLabel the column label
	 * @return the value
	 */
	public Date getDate(String columnLabel) throws SQLException {
		return getDate(findColumn(columnLabel));
	}

	/**
	 * INTERNAL
	 */
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * INTERNAL
	 */
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * Returns the value as a double.
	 *
	 * @param columnIndex (1,2,...)
	 * @return the value
	 */
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
	public double getDouble(String columnLabel) throws SQLException {
		return getDouble(findColumn(columnLabel));
	}

	/**
	 * Returns the value as a float.
	 *
	 * @param columnIndex (1,2,...)
	 * @return the value
	 */
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
	public float getFloat(String columnLabel) throws SQLException {
		return getFloat(findColumn(columnLabel));
	}

	/**
	 * Returns the value as an int.
	 *
	 * @param columnIndex (1,2,...)
	 * @return the value
	 */
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
	public int getInt(String columnLabel) throws SQLException {
		return getInt(findColumn(columnLabel));
	}

	/**
	 * Returns the value as a long.
	 *
	 * @param columnIndex (1,2,...)
	 * @return the value
	 */
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
	public long getLong(String columnLabel) throws SQLException {
		return getLong(findColumn(columnLabel));
	}

	/**
	 * INTERNAL
	 */
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * INTERNAL
	 */
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * INTERNAL
	 */
	public NClob getNClob(int columnIndex) throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * INTERNAL
	 */
	public NClob getNClob(String columnLabel) throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * INTERNAL
	 */
	public String getNString(int columnIndex) throws SQLException {
		return getString(columnIndex);
	}

	/**
	 * INTERNAL
	 */
	public String getNString(String columnLabel) throws SQLException {
		return getString(columnLabel);
	}

	/**
	 * Returns the value as an Object.
	 *
	 * @param columnIndex (1,2,...)
	 * @return the value
	 */
	public Object getObject(int columnIndex) throws SQLException {
		return get(columnIndex);
	}

	/**
	 * Returns the value as an Object.
	 *
	 * @param columnLabel the column label
	 * @return the value
	 */
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
	public <T> T getObject(String columnName, Class<T> type) throws SQLException {
		return getObject(findColumn(columnName), type);
	}

	/**
	 * INTERNAL
	 */
	public Object getObject(int columnIndex, Map<String, Class<?>> map)
			throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * INTERNAL
	 */
	public Object getObject(String columnLabel, Map<String, Class<?>> map)
			throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * INTERNAL
	 */
	public Ref getRef(int columnIndex) throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * INTERNAL
	 */
	public Ref getRef(String columnLabel) throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * INTERNAL
	 */
	public RowId getRowId(int columnIndex) throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * INTERNAL
	 */
	public RowId getRowId(String columnLabel) throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * Returns the value as a short.
	 *
	 * @param columnIndex (1,2,...)
	 * @return the value
	 */
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
	public short getShort(String columnLabel) throws SQLException {
		return getShort(findColumn(columnLabel));
	}

	/**
	 * INTERNAL
	 */
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * INTERNAL
	 */
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * Returns the value as a String.
	 *
	 * @param columnIndex (1,2,...)
	 * @return the value
	 */
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
	public String getString(String columnLabel) throws SQLException {
		return getString(findColumn(columnLabel));
	}

	/**
	 * Returns the value as a java.sql.Time.
	 *
	 * @param columnIndex (1,2,...)
	 * @return the value
	 */
	public Time getTime(int columnIndex) throws SQLException {
		return (Time) get(columnIndex);
	}

	/**
	 * Returns the value as a java.sql.Time.
	 *
	 * @param columnLabel the column label
	 * @return the value
	 */
	public Time getTime(String columnLabel) throws SQLException {
		return getTime(findColumn(columnLabel));
	}

	/**
	 * INTERNAL
	 */
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * INTERNAL
	 */
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * Returns the value as a java.sql.Timestamp.
	 *
	 * @param columnIndex (1,2,...)
	 * @return the value
	 */
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return (Timestamp) get(columnIndex);
	}

	/**
	 * Returns the value as a java.sql.Timestamp.
	 *
	 * @param columnLabel the column label
	 * @return the value
	 */
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return getTimestamp(findColumn(columnLabel));
	}

	/**
	 * INTERNAL
	 */
	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * INTERNAL
	 */
	public Timestamp getTimestamp(String columnLabel, Calendar cal)
			throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * @deprecated INTERNAL
	 */
	@Deprecated
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * @deprecated INTERNAL
	 */
	@Deprecated
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * INTERNAL
	 */
	public URL getURL(int columnIndex) throws SQLException {
		throw getUnsupportedException();
	}

	/**
	 * INTERNAL
	 */
	public URL getURL(String columnLabel) throws SQLException {
		throw getUnsupportedException();
	}

	static SQLException getUnsupportedException() {
		return DbException.getJdbcSQLException(ErrorCode.FEATURE_NOT_SUPPORTED_1);
	}
}
