package net.xdob.ratly.protocol;

import java.util.Objects;

public class BeanTarget<T> {
  private final Class<T> clazz;
  private final String beanName;

  public BeanTarget(Class<T> clazz, String beanName) {
    this.clazz = clazz;
    this.beanName = beanName;
  }

  public Class<T> getClazz() {
    return clazz;
  }

  public String getBeanName() {
    return beanName;
  }

  @Override
  public boolean equals(Object object) {
    if (object == null || getClass() != object.getClass()) return false;
    BeanTarget<?> that = (BeanTarget<?>) object;
    return Objects.equals(clazz, that.clazz) && Objects.equals(beanName, that.beanName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clazz, beanName);
  }

  @Override
  public String toString() {
    return clazz + "#" + beanName;
  }

  public static <T> BeanTarget<T> valueOf(Class<T> clazz){
    return valueOf(clazz, null);
  }

  public static <T> BeanTarget<T> valueOf(Class<T> clazz, String beanName){
    if(clazz==null){
      return null;
    }
    return new BeanTarget<T>(clazz, beanName);
  }
}
