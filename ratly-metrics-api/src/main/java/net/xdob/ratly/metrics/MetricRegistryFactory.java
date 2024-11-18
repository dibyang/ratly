
package net.xdob.ratly.metrics;

/**
 * A Factory for creating MetricRegistries. This is the main plugin point for metrics implementation
 */
public interface MetricRegistryFactory {
  /**
   * Create a MetricRegistry from the given MetricRegistryInfo
   * @param info the descriptor for MetricRegistry
   * @return a MetricRegistry implementation
   */
  RatlyMetricRegistry create(MetricRegistryInfo info);
}
