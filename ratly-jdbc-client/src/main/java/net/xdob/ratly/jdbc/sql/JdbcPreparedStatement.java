package net.xdob.ratly.jdbc.sql;


import net.xdob.ratly.jdbc.*;
import net.xdob.ratly.jdbc.util.Streams4Jdbc;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JdbcPreparedStatement extends JdbcStatement implements PreparedStatement {
  protected Parameters parameters = new Parameters();
  protected final ArrayList<Parameters> batchParameters = new ArrayList<>();
  protected final String sql;

  protected SerialResultSetMetaData resultSetMetaData;
  protected SerialParameterMetaData parameterMetaData;

  public JdbcPreparedStatement(SqlClient client, String sql) {
    super(client);
    this.sql = sql;
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    QueryRequest queryRequest = newQueryRequest()
        .setSql(sql)
        .addParams(parameters);
    return sendQuery(queryRequest);
  }

  @Override
  protected QueryRequest newQueryRequest() {
    return super.newQueryRequest()
        .setSender(Sender.prepared);
  }

  @Override
  protected UpdateRequest newUpdateRequest() {
    return super.newUpdateRequest()
        .setSender(Sender.prepared);
  }


  @Override
  public long executeLargeUpdate() throws SQLException {
    UpdateRequest updateRequest = newUpdateRequest()
        .setSql(sql)
        .addParams(parameters);
    return sendUpdate(updateRequest);
  }

  @Override
  public long[] executeLargeBatch() throws SQLException {
    List<Long> batchCounts = new ArrayList<>();
    int start = 0;
    UpdateRequest updateRequest = newUpdateRequest()
        .setSql(sql);
    while (start < batchParameters.size()) {
      updateRequest.getBatchParams().clear();
      batchParameters.stream().skip(start)
          .limit(limit)
          .forEach(updateRequest::addBatchParams);
      start += updateRequest.getBatchParams().size();
      long[] counts = sendUpdateBatch(updateRequest);
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
    return parameters.getOrCreate(parameterIndex);
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
    parameters.clear();
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
    if(isQuery(sql)){
      resultSet = this.executeQuery();
    }else {
      updateCount = this.executeUpdate();
    }
    return true;
  }

  @Override
  public void addBatch() throws SQLException {
    synchronized (batchParameters){
      batchParameters.add(parameters);
      parameters = new Parameters();
    }
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
    if(resultSetMetaData==null) {
      getMeta();
    }
    return this.resultSetMetaData;
  }

  private void getMeta() throws SQLException {
    QueryRequest queryRequest = new QueryRequest()
        .setSender(Sender.prepared)
        .setTx(sqlClient.getTx())
        .setDb(sqlClient.getCi().getDb())
        .setType(QueryType.meta).setSql(sql);
    QueryReply queryReply = sendQueryRequest(queryRequest);
    if (queryReply.getEx()==null) {
      if(queryReply.getRsMeta()!=null) {
        resultSetMetaData = (SerialResultSetMetaData) queryReply.getRsMeta();
      }
      parameterMetaData = (SerialParameterMetaData) queryReply.getParamMeta();
    } else {
      throw queryReply.getEx();
    }
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
    if(parameterMetaData==null) {
      getMeta();
    }
    return this.parameterMetaData;
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
