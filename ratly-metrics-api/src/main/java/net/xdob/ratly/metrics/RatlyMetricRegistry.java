package net.xdob.ratly.metrics;

import java.util.Optional;
import java.util.function.Supplier;

public interface RatlyMetricRegistry {
  // 记录指定名称的时间消耗
  Timekeeper timer(String name);

  // 记录指定名称的计数值
  LongCounter counter(String name);

  // 移除指定名称的度量
  boolean remove(String name);

  // 记录指定名称的瞬时值
  <T> void gauge(String name, Supplier<Supplier<T>> gaugeSupplier);

  // 获取度量注册表的信息
  MetricRegistryInfo getMetricRegistryInfo();

  default <T> Optional<T> wrap(Class<T> clazz){
    if(clazz.isAssignableFrom(this.getClass())){
      return Optional.ofNullable((T) this);
    }
    return Optional.empty();
  }
}