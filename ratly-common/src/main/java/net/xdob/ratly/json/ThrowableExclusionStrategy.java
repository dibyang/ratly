package net.xdob.ratly.json;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;

public class ThrowableExclusionStrategy implements ExclusionStrategy {

  @Override
  public boolean shouldSkipField(FieldAttributes f) {
    // TODO Auto-generated method stub
    if (Throwable.class.isAssignableFrom(f.getDeclaringClass())) {
      return f.getName().equals("cause") || f.getName().equals("suppressedExceptions");
    }
    return false;
  }

  @Override
  public boolean shouldSkipClass(Class<?> clazz) {
    // TODO Auto-generated method stub
    return false;
  }

}
