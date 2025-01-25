package net.xdob.ratly.rmap;

import java.io.IOException;
import java.util.List;

public interface DFactory {
  List<CacheInfo> list() throws IOException;
  <V> RMap<V> getOrcreateRMap(String name) throws IOException;
  CacheInfo drop(String name) throws IOException;
}
