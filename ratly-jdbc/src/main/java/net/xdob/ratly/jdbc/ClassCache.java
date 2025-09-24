package net.xdob.ratly.jdbc;

import com.google.common.collect.Maps;

import java.lang.reflect.Method;
import java.util.Map;

public class ClassCache {
  private final Map<Class<?>,MethodCache> cache = Maps.newHashMap();

  public Method getMethod(Class<?> clazz,String methodName, Class<?>[] paramTypes) throws NoSuchMethodException{
    return cache.computeIfAbsent(clazz, MethodCache::new)
        .getMethod(methodName, paramTypes);
  }
}
