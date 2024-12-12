package net.xdob.ratly.jdbc.sql;

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class SimpleRow implements Serializable {
  private final LinkedList<Object> values = Lists.newLinkedList();

  public SimpleRow() {
  }

  public SimpleRow(int cols) {
    setColumns(cols);
  }

  public List<Object> getValues() {
    return values;
  }

  public SimpleRow setColumns(int cols){
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

  public SimpleRow setValue(int index, Object value){
    if(index>= getColumns()){
      setColumns(index+1);
    }
    values.set(index, value);
    return this;
  }

  @Override
  public String toString() {
    return values.toString();
  }
}
