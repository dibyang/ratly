package net.xdob.ratly.jdbc.sql;

import com.google.common.collect.Lists;
import net.xdob.ratly.jdbc.util.Streams4Jdbc;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.io.*;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

public class SerialRow implements Serializable {
  private final LinkedList<Object> values = Lists.newLinkedList();

  public SerialRow() {
  }

  public SerialRow(int cols) {
    setColumns(cols);
  }

  public List<Object> getValues() {
    return values;
  }

  public SerialRow setColumns(int cols){
    while(cols>values.size()){
      values.addLast(null);
    }
    while (cols<values.size()){
      values.removeLast();
    }
    return this;
  }

  public int getColumns(){
    return values.size();
  }

  public Object getValue(int index){
    return values.get(index);
  }

  public SerialRow setValue(int index, Object value) throws SQLException {
    if(index>= getColumns()){
      setColumns(index+1);
    }
    values.set(index, getValue(value));
    return this;
  }

  private Object getValue(Object value) throws SQLException {
    Object v = value;
    if(value instanceof Clob){
      Clob clob = (Clob) value;
      v = Streams4Jdbc.readString(clob.getCharacterStream());
    } else if(value instanceof Blob){
      Blob blob = (Blob) value;
      v = Streams4Jdbc.readBytes(blob.getBinaryStream());
    }
    return v;
  }

  @Override
  public String toString() {
    return values.toString();
  }
}
