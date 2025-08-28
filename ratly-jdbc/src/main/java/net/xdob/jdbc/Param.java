package net.xdob.jdbc;

import java.io.Serializable;

public class Param implements Serializable {
  private int index;
  private Object value;

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }
}
