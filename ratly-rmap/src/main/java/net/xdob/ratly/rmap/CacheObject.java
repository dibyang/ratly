package net.xdob.ratly.rmap;

import java.util.*;

public class CacheObject {
  private final Map<String,Object> map;
  private final String name;

  public CacheObject(String name) {
    this.name = name;
    this.map = Collections.synchronizedMap(new HashMap<>());
  }

  public String getName() {
    return name;
  }



  public Map<String, Object> getMap() {
    return map;
  }


  public int size(){
    return map.size();
  }

  public CacheInfo toCacheInfo(){
    return new CacheInfo(this.name, this.size());
  }

  public static CacheObject newMap(String name){
    return new CacheObject(name);
  }

}
