package net.xdob.ratly.rmap;

import net.xdob.ratly.rmap.function.SerialGetFun;

public class GetRequest {
  private GetMethod method = GetMethod.get;
  private String name;
  private String key;
  private SerialGetFun fun;

  public GetMethod getMethod() {
    return method;
  }

  public GetRequest setMethod(GetMethod method) {
    this.method = method;
    return this;
  }

  public String getName() {
    return name;
  }

  public GetRequest setName(String name) {
    this.name = name;
    return this;
  }

  public String getKey() {
    return key;
  }

  public GetRequest setKey(String key) {
    this.key = key;
    return this;
  }

  public <R> SerialGetFun<R> getFun() {
    return fun;
  }

  public <R> GetRequest setFun(SerialGetFun<R> fun) {
    this.fun = fun;
    return this;
  }
}
