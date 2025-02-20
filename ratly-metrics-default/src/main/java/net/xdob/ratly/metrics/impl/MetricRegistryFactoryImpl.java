package net.xdob.ratly.metrics.impl;

import net.xdob.ratly.metrics.MetricRegistryFactory;
import net.xdob.ratly.metrics.MetricRegistryInfo;
import net.xdob.ratly.metrics.RatlyMetricRegistry;

public class MetricRegistryFactoryImpl implements MetricRegistryFactory {
  @Override
  public RatlyMetricRegistry create(MetricRegistryInfo info) {
    return new RatlyMetricRegistryImpl(info);
  }
}
