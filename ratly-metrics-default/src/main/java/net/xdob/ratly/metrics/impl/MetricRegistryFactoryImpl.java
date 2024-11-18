package net.xdob.ratly.metrics.impl;

import net.xdob.ratly.metrics.MetricRegistryFactory;
import net.xdob.ratly.metrics.MetricRegistryInfo;

public class MetricRegistryFactoryImpl implements MetricRegistryFactory {
  @Override
  public RatlyMetricRegistryImpl create(MetricRegistryInfo info) {
    return new RatlyMetricRegistryImpl(info);
  }
}
