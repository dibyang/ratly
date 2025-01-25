package net.xdob.ratly.rmap;

import net.xdob.ratly.rmap.function.SerialGetFun;
import net.xdob.ratly.rmap.function.SerialPutFun;

import java.io.IOException;
import java.util.Map;

public class RMap<V> {
  private final DContext context;
  private final String name;
  public RMap(DContext context, String name) throws IOException {
    this.context = context;
    this.name = name;
    PutRequest putRequest = new PutRequest()
        .setMethod(PutMethod.create)
        .setName(name);
    context.sendPutRequest(putRequest);
  }

  public String getName() {
    return name;
  }


  public int size() throws IOException {
    GetRequest getRequest = newGetRequest()
        .setMethod(GetMethod.size);
    GetReply getReply = context.sendGetRequest(getRequest);
    return getReply.getSize();
  }

  private GetRequest newGetRequest() {
    return new GetRequest().setName(name);
  }

  public V get(String key) throws IOException {
    GetRequest getRequest = newGetRequest()
        .setMethod(GetMethod.get)
        .setKey(key);
    GetReply getReply = context.sendGetRequest(getRequest);
    return (V)getReply.getData();
  }

  public V get(String key, SerialGetFun<V> fun) throws IOException {
    GetRequest getRequest = newGetRequest()
        .setMethod(GetMethod.get)
        .setKey(key).setFun(fun);
    GetReply getReply = context.sendGetRequest(getRequest);
    return (V)getReply.getData();
  }

  public V remove(String key) throws IOException {
    PutRequest putRequest = new PutRequest()
        .setMethod(PutMethod.remove)
        .setName(name)
        .setKey(key);
    PutReply putReply = context.sendPutRequest(putRequest);
    return (V)putReply.getData();
  }

  public void put(String key, V value) throws IOException {
    PutRequest putRequest = new PutRequest()
        .setMethod(PutMethod.put)
        .setName(name)
        .setKey(key)
        .setData(value);
    context.sendPutRequest(putRequest);
  }

  public void put(String key, V value, SerialPutFun<V> fun) throws IOException {
    PutRequest putRequest = new PutRequest()
        .setMethod(PutMethod.put)
        .setName(name)
        .setKey(key)
        .setData(value)
        .setFun(fun);
    context.sendPutRequest(putRequest);
  }

  public void replaceAll(Map<String,V> data) throws IOException {
    PutRequest putRequest = new PutRequest()
        .setMethod(PutMethod.put)
        .setName(name)
        .setData(data)
        .setFun((cache, d)->{
          Map<String,V> map = (Map<String,V>)d;
          cache.getMap().clear();
          cache.getMap().putAll(map);
          return null;
        });
    context.sendPutRequest(putRequest);
  }

  public void putAll(Map<String,V> data) throws IOException {
    PutRequest putRequest = new PutRequest()
        .setMethod(PutMethod.put)
        .setName(name)
        .setData(data)
        .setFun((cache, d)->{
          Map<String,V> map = (Map<String,V>)d;
          cache.getMap().putAll(map);
          return null;
        });
    context.sendPutRequest(putRequest);
  }

  public void clear() throws IOException {
    PutRequest putRequest = new PutRequest()
        .setMethod(PutMethod.put)
        .setName(name)
        .setFun((cache, d)->{
          cache.getMap().clear();
          return null;
        });
    context.sendPutRequest(putRequest);
  }

}
