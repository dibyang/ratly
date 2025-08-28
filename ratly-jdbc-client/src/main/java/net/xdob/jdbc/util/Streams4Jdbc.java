package net.xdob.jdbc.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.SQLException;

public abstract class Streams4Jdbc {
  public static byte[] readBytes(InputStream inputStream) throws SQLException {
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

  public static byte[] readBytes(InputStream inputStream, int length) throws SQLException {
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

  public static String readString(Reader reader, int length) throws SQLException {
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

  public static String readString(Reader reader) throws SQLException {
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
}
