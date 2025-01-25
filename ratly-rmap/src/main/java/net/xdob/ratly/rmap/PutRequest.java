package net.xdob.ratly.rmap;

import net.xdob.ratly.rmap.function.SerialPutFun;

public class PutRequest {
  private PutMethod method;
  private String name;
  private String key;
  private Object data;
  private SerialPutFun fun;

  public PutMethod getMethod() {
    return method;
  }

  public PutRequest setMethod(PutMethod method) {
    this.method = method;
    return this;
  }

  public String getName() {
    return name;
  }

  public PutRequest setName(String name) {
    this.name = name;
    return this;
  }

  public String getKey() {
    return key;
  }

  public PutRequest setKey(String key) {
    this.key = key;
    return this;
  }

  public Object getData() {
    return data;
  }

  public PutRequest setData(Object data) {
    this.data = data;
    return this;
  }

  public <R> SerialPutFun<R> getFun() {
    return fun;
  }

  public <R> PutRequest setFun(SerialPutFun<R> fun) {
    this.fun = fun;
    return this;
  }
}
