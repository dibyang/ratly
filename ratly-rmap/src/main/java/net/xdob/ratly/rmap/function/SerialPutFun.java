package net.xdob.ratly.rmap.function;

import net.xdob.ratly.rmap.CacheObject;

import java.io.Serializable;

@FunctionalInterface
public interface SerialPutFun<R> extends Serializable {
  R apply(CacheObject cache, Object data);
}
