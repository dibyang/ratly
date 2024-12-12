package net.xdob.ratly.jdbc.sql;


import net.xdob.ratly.proto.jdbc.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class JdbcPreparedStatement extends JdbcStatement implements PreparedStatement {
  protected Parameters parameters = new Parameters();
  protected final ArrayList<Parameters> batchParameters = new ArrayList<>();
  protected final String sql;

  protected SimpleResultSetMetaData resultSetMetaData;
  protected SimpleParameterMetaData parameterMetaData;

  public JdbcPreparedStatement(SqlClient client, String sql) {
    super(client);
    this.sql = sql;
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    QueryRequestProto queryRequestProto = QueryRequestProto.newBuilder()
        .setSender(Sender.prepared)
        .setType(QueryType.query)
        .setTx(sqlClient.getTx())
        .setDb(sqlClient.getCi().getDb())
        .setSql(sql)
        .setParam(parameters.toParamListProto(sqlClient.getFasts()))
        .build();
    return sendQuery(queryRequestProto);
  }

  private UpdateRequestProto.Builder getUpdateRequest() {
    return UpdateRequestProto.newBuilder()
        .setSender(Sender.prepared)
        .setTx(sqlClient.getTx())
        .setDb(sqlClient.getCi().getDb());
  }

  @Override
  public long executeLargeUpdate() throws SQLException {
    UpdateRequestProto.Builder updateRequest = getUpdateRequest()
        .setSql(sql)
        .setParam(parameters.toParamListProto(sqlClient.getFasts()));
    return sendUpdate(updateRequest.build());
  }

  @Override
  public long[] executeLargeBatch() throws SQLException {
    UpdateRequestProto.Builder updateRequest = getUpdateRequest()
        .setSql(sql);
    for (int i = 0; i < batchParameters.size(); i++) {
      updateRequest.setBatchParam(i, batchParameters.get(i).toParamListProto(sqlClient.getFasts()));
    }

    return sendUpdateBatch(updateRequest.build());
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
    return parameters.computeIfAbsent(parameterIndex, Parameter::c);
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

  public byte[] readBytes(InputStream inputStream, int length) throws SQLException {
    try{
      byte[] buffer = new byte[length];
      int bytesRead = 0;
      // 读取数据
      while (bytesRead < length) {
        int read = inputStream.read(buffer, bytesRead, length - bytesRead);
        if (read == -1) {
          break; // 文件结束
        }
        bytesRead += read;
      }
      return buffer;
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }

  public byte[] readBytes(InputStream inputStream) throws SQLException {
    try(ByteArrayOutputStream bos = new ByteArrayOutputStream()){
      byte[] buffer = new byte[1024];
      int bytesRead;
      while ((bytesRead = inputStream.read(buffer)) != -1) {
        bos.write(buffer, 0, bytesRead);
      }
      return bos.toByteArray();
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    String val = new String(readBytes(x, length));
    setObject(parameterIndex, val);
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw unsupported("unicodeStream");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    byte[] bytes = readBytes(x, length);
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
  public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
    String s = readString(reader, length);
    setObject(parameterIndex, s);
  }

  private String readString(Reader reader, int length) throws SQLException {
    String s = null;
    try  {
      char[] buffer = new char[length];
      int len = reader.read(buffer,0 , length);
      s = new String(buffer, 0, len);
    } catch (IOException e) {
      throw new SQLException(e);
    }
    return s;
  }

  private String readString(Reader reader) throws SQLException {
    StringBuilder buffer = new StringBuilder();
    try  {
      char[] chars = new char[1024];
      int len = 0;
      while((len = reader.read(chars,0 , 1024))!=-1){
        buffer.append(chars, 0, len);
      }
    } catch (IOException e) {
      throw new SQLException(e);
    }
    return buffer.toString();
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
   throw unsupported("ref");
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    if(x!=null) {
      byte[] bytes = x.getBytes(0, (int) x.length());
      setObject(parameterIndex, bytes);
    }else{
      setObject(parameterIndex, null);
    }
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    if(x!=null) {
      String s = readString(x.getCharacterStream(), (int) x.length());
      setObject(parameterIndex, s);
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
    QueryRequestProto queryRequest = QueryRequestProto.newBuilder()
        .setSender(Sender.prepared)
        .setTx(sqlClient.getTx())
        .setDb(sqlClient.getCi().getDb())
        .setType(QueryType.meta).setSql(sql)
        .build();
    QueryReplyProto queryReplyProto = sendQueryRequest(queryRequest);
    if (!queryReplyProto.hasEx()) {
      resultSetMetaData = (SimpleResultSetMetaData) sqlClient.getFasts().asObject(queryReplyProto.getRsMeta().toByteArray());
      parameterMetaData = (SimpleParameterMetaData) sqlClient.getFasts().asObject(queryReplyProto.getParamMeta().toByteArray());
    } else {
      throw getSQLException(queryReplyProto.getEx());
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
    throw unsupported("ref");
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
    throw unsupported("rowId");
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    setObject(parameterIndex, value);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
    String s = readString(value, (int)length);
    setObject(parameterIndex, s);
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    if(value!=null) {
      String s = readString(value.getCharacterStream(), (int) value.length());
      setObject(parameterIndex, s);
    }else {
      setObject(parameterIndex,null);
    }
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    String s = readString(reader, (int)length);
    setObject(parameterIndex, s);
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
    byte[] bytes = readBytes(inputStream, (int) length);
    setObject(parameterIndex, bytes);
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    String s = readString(reader, (int)length);
    setObject(parameterIndex, s);
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    if(xmlObject!=null) {
      String s = readString(xmlObject.getCharacterStream());
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
    byte[] bytes = readBytes(x);
    setObject(parameterIndex, bytes);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    byte[] bytes = readBytes(x);
    setObject(parameterIndex, bytes);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
    String s = readString(reader);
    setObject(parameterIndex, s);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    byte[] bytes = readBytes(x);
    setObject(parameterIndex, bytes);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    byte[] bytes = readBytes(x);
    setObject(parameterIndex, bytes);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    String s = readString(reader);
    setObject(parameterIndex, s);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    String s = readString(value);
    setObject(parameterIndex, s);
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    String s = readString(reader);
    setObject(parameterIndex, s);
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    byte[] bytes = readBytes(inputStream);
    setObject(parameterIndex, bytes);
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    String s = readString(reader);
    setObject(parameterIndex, s);
  }

}
