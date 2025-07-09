package net.xdob.ratly.jdbc.sql;


import java.io.Serializable;


public class Parameter implements Serializable {
  private final int index;
  private Object value;

  public Parameter(int index) {
    this.index = index;
  }

  public int getIndex() {
    return index;
  }

  public Object getValue() {
    return value;
  }

  public Parameter setValue(Object value) {
    this.value = value;
    return this;
  }


  public static Parameter c(int parameterIndex){
    return new Parameter(parameterIndex);
  }
}
