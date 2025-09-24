package net.xdob.ratly.jdbc;

import com.google.common.collect.Maps;

import java.lang.reflect.Method;
import java.util.Map;

public class MethodCache {

  private final Map<MethodKey,Method> cache = Maps.newHashMap();
  private final Class<?> clazz;

  public MethodCache(Class<?> clazz) {
    this.clazz = clazz;
  }

  public Method getMethod(String methodName, Class<?>[] paramTypes) throws NoSuchMethodException{
    MethodKey methodKey = MethodKey.of(methodName, paramTypes);
    synchronized (cache){
      Method method = cache.get(methodKey);
      if(method==null){
        method = clazz.getMethod(methodKey.getMethodName(), methodKey.getParamTypes());
        cache.put(methodKey, method);
      }
      return method;
    }
  }
}
