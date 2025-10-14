package net.xdob.ratly.jdbc.sql;


import net.xdob.ratly.util.Streams4Jdbc;
import net.xdob.ratly.proto.jdbc.*;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;


public class JdbcPreparedStatement extends JdbcStatement implements PreparedStatement {

  protected final List<Parameters> batchParameters = Collections.synchronizedList(new ArrayList<>());
  protected final String sql;

  protected SerialResultSetMetaData resultSetMetaData;
  protected SerialParameterMetaData parameterMetaData;
  protected AtomicReference<Parameters> parametersRef = new AtomicReference<>(new Parameters());

  public JdbcPreparedStatement(SqlClient client, String sql) {
    super(client);
    this.sql = sql;
  }

  @Override
  protected SqlRequestProto.Builder newSqlRequestBuilder(SqlRequestType type) {
    return super.newSqlRequestBuilder(type)
        .setStmtType(StmtType.prepared);
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    SqlRequestProto.Builder sqlBuilder = newSqlRequestBuilder(SqlRequestType.query);
    sqlBuilder.setSql( sql);
    sqlBuilder.setParams(parametersRef.get().toProto());
    return sendQuery(sqlBuilder);
  }

  @Override
  public long executeLargeUpdate() throws SQLException {
    SqlRequestProto.Builder sqlBuilder = newSqlRequestBuilder(SqlRequestType.update);
    sqlBuilder.setSql( sql);
    sqlBuilder.setParams(parametersRef.get().toProto());
    return sendUpdate(sqlBuilder);
  }

  @Override
  public long[] executeLargeBatch() throws SQLException {
    List<Long> batchCounts = new ArrayList<>();
    int start = 0;
    SqlRequestProto.Builder sqlBuilder = newSqlRequestBuilder(SqlRequestType.update);
    sqlBuilder.setSql( sql);
    while (start < batchParameters.size()) {
      sqlBuilder.clearBatchParams();
      List<Parameters> parameters = batchParameters.stream().skip(start)
          .limit(limit)
          .collect(Collectors.toList());
      for (Parameters parameter : parameters) {
        sqlBuilder.addBatchParams(parameter.toProto());
      }
      start += sqlBuilder.getBatchParamsCount();
      long[] counts = sendUpdateBatch(sqlBuilder);
      for (long count : counts) {
        batchCounts.add(count);
      }
    }
    return batchCounts.stream()
        .mapToLong(Long::longValue)
        .toArray() ;
  }

  @Override
  public int executeUpdate() throws SQLException {
    long updateCount = executeLargeUpdate();
    return count4int(updateCount);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    getParameter(parameterIndex)
        .setValue(null);
  }

  private Parameter getParameter(int parameterIndex) {
    return parametersRef.get().getOrCreate(parameterIndex);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    String val = new String(Streams4Jdbc.readBytes(x, length));
    setObject(parameterIndex, val);
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw unsupported("unicodeStream");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    byte[] bytes = Streams4Jdbc.readBytes(x, length);
    setObject(parameterIndex, bytes);
  }

  @Override
  public void clearParameters() throws SQLException {
    parametersRef.get().clear();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    getParameter(parameterIndex)
        .setValue(x);
  }

  @Override
  public boolean execute() throws SQLException {
    if(isModification(sql)){
			updateCount = this.executeUpdate();
    }else {
			resultSet = this.executeQuery();
    }
    return true;
  }

  @Override
  public void addBatch() throws SQLException {
    Parameters old = parametersRef.getAndUpdate(e -> new Parameters());
    batchParameters.add(old);
  }

  @Override
  public void clearBatch() throws SQLException {
    super.clearBatch();
    batchParameters.clear();
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
    String s = Streams4Jdbc.readString(reader, length);
    setObject(parameterIndex, s);
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
   throw unsupported("ref");
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    if(x!=null) {
      byte[] bytes = x.getBytes(0, (int) x.length());
      setObject(parameterIndex, new SerialBlob(bytes));
    }else{
      setObject(parameterIndex, null);
    }
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    if(x!=null) {
      String s = Streams4Jdbc.readString(x.getCharacterStream(), (int) x.length());
      setObject(parameterIndex, new SerialClob(s.toCharArray()));
    }else{
      setObject(parameterIndex, null);
    }
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    SqlRequestProto.Builder sqlRequestBuilder = newSqlRequestBuilder(SqlRequestType.resultSetMetaData);
    JdbcRequestProto requestProto = newRequestBuilder()
        .setSqlRequest(sqlRequestBuilder)
        .build();
    JdbcResponseProto responseProto = sendReadOnly(requestProto);

    return SerialResultSetMetaData.from(responseProto.getResultSet().getColumnsList());
  }


  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    setObject(parameterIndex, null);
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    throw unsupported("url");
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    SqlRequestProto.Builder sqlRequestBuilder = newSqlRequestBuilder(SqlRequestType.parameterMeta);
    JdbcRequestProto requestProto = newRequestBuilder()
        .setSqlRequest(sqlRequestBuilder)
        .build();
    JdbcResponseProto responseProto = sendReadOnly(requestProto);

    return SerialParameterMetaData.from(responseProto.getParameterMeta());
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    setObject(parameterIndex, new SerialRowId(x));
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    setObject(parameterIndex, value);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
    String s = Streams4Jdbc.readString(value, (int)length);
    setObject(parameterIndex, s);
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    if(value!=null) {
      //String s = Streams4Jdbc.readString(value.getCharacterStream());
      SerialClob clob = new SerialClob(value);
      setObject(parameterIndex, clob);
    }else {
      setObject(parameterIndex,null);
    }
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    String s = Streams4Jdbc.readString(reader, (int)length);
    SerialClob clob = new SerialClob(s.toCharArray());
    setObject(parameterIndex, clob);
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
    byte[] bytes = Streams4Jdbc.readBytes(inputStream, (int) length);
    SerialBlob blob = new SerialBlob(bytes);
    setObject(parameterIndex, blob);
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    String s = Streams4Jdbc.readString(reader, (int)length);
    SerialClob clob = new SerialClob(s.toCharArray());
    setObject(parameterIndex, clob);
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    if(xmlObject!=null) {
      String s = Streams4Jdbc.readString(xmlObject.getCharacterStream());
      setObject(parameterIndex, s);
    }else {
      setObject(parameterIndex, null);
    }
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    byte[] bytes = Streams4Jdbc.readBytes(x);
    setObject(parameterIndex, bytes);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    byte[] bytes = Streams4Jdbc.readBytes(x);
    setObject(parameterIndex, bytes);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
    String s = Streams4Jdbc.readString(reader);
    setObject(parameterIndex, s);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    byte[] bytes = Streams4Jdbc.readBytes(x);
    setObject(parameterIndex, new String(bytes));
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    byte[] bytes = Streams4Jdbc.readBytes(x);
    SerialBlob blob = new SerialBlob(bytes);
    setObject(parameterIndex, blob);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    String s = Streams4Jdbc.readString(reader);
    setObject(parameterIndex, s);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    String s = Streams4Jdbc.readString(value);
    setObject(parameterIndex, s);
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    String s = Streams4Jdbc.readString(reader);
    SerialClob clob = new SerialClob(s.toCharArray());
    setObject(parameterIndex, clob);
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    byte[] bytes = Streams4Jdbc.readBytes(inputStream);
    SerialBlob blob = new SerialBlob(bytes);
    setObject(parameterIndex, blob);
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    String s = Streams4Jdbc.readString(reader);
    SerialClob clob = new SerialClob(s.toCharArray());
    setObject(parameterIndex, clob);
  }

}
