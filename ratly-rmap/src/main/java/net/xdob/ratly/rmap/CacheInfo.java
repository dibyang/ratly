package net.xdob.ratly.rmap;

import java.util.Objects;

public class CacheInfo {
  private final String name;
  private final int size;

  public CacheInfo(String name, int size) {
    this.name = name;
    this.size = size;
  }

  public String getName() {
    return name;
  }


  public int getSize() {
    return size;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    CacheInfo cacheInfo = (CacheInfo) o;
    return Objects.equals(name, cacheInfo.name) ;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
