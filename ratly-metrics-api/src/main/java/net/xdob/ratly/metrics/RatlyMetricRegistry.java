
package net.xdob.ratly.metrics;

import java.util.function.Supplier;

public interface RatlyMetricRegistry {
  Timekeeper timer(String name);

  LongCounter counter(String name);

  boolean remove(String name);

  <T> void gauge(String name, Supplier<Supplier<T>> gaugeSupplier);

  MetricRegistryInfo getMetricRegistryInfo();
}
