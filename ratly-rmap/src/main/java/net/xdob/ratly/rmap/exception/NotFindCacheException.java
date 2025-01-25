package net.xdob.ratly.rmap.exception;

import java.io.IOException;

public class NotFindCacheException extends IOException {

  public NotFindCacheException(String name) {
    super("cache " + name + "not find.");
  }
}
