package net.xdob.jdbc;


import java.util.Arrays;
import java.util.Objects;

public class MethodKey {
  private final String methodName;
  private final Class<?>[] paramTypes;

  public MethodKey(String methodName, Class<?>[] paramTypes) {
    this.methodName = methodName;
    this.paramTypes = paramTypes;
  }

  public String getMethodName() {
    return methodName;
  }

  public Class<?>[] getParamTypes() {
    return paramTypes;
  }

  @Override
  public boolean equals(Object object) {
    if (object == null || getClass() != object.getClass()) return false;
    MethodKey methodKey = (MethodKey) object;
    return Objects.equals(methodName, methodKey.methodName) && Objects.deepEquals(paramTypes, methodKey.paramTypes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(methodName, Arrays.hashCode(paramTypes));
  }

  public static MethodKey of(String methodName, Class<?>[] paramTypes){
    return new MethodKey(methodName, paramTypes);
  }
}
