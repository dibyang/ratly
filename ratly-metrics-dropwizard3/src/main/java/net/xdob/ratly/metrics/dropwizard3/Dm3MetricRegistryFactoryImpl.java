package net.xdob.ratly.metrics.dropwizard3;

import net.xdob.ratly.metrics.MetricRegistryFactory;
import net.xdob.ratly.metrics.MetricRegistryInfo;

public class Dm3MetricRegistryFactoryImpl implements MetricRegistryFactory {
  @Override
  public Dm3RatlyMetricRegistryImpl create(MetricRegistryInfo info) {
    return new Dm3RatlyMetricRegistryImpl(info);
  }
}
