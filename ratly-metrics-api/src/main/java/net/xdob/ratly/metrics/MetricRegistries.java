
package net.xdob.ratly.metrics;

import net.xdob.ratly.util.TimeDuration;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/**
 * MetricRegistries is collection of MetricRegistry's. MetricsRegistries implementations should do
 * ref-counting of MetricRegistry's via create() and remove() methods.
 */
public abstract class MetricRegistries {

  private static final class LazyHolder {
    private static final MetricRegistries GLOBAL = MetricRegistriesLoader.load();
  }

  public abstract String name();
  /**
   * Return the global singleton instance for the MetricRegistries.
   *
   * @return MetricRegistries implementation.
   */
  public static MetricRegistries global() {
    return LazyHolder.GLOBAL;
  }

  /**
   * Removes all the MetricRegisties.
   */
  public abstract void clear();

  /**
   * Create or return MetricRegistry with the given info. MetricRegistry will only be created
   * if current reference count is 0. Otherwise ref counted is incremented, and an existing instance
   * will be returned.
   *
   * @param info the info object for the MetricRegistrytry.
   * @return created or existing MetricRegistry.
   */
  public abstract RatlyMetricRegistry create(MetricRegistryInfo info);

  /**
   * Decrements the ref count of the MetricRegistry, and removes if ref count == 0.
   *
   * @param key the info object for the MetricRegistrytry.
   * @return true if metric registry is removed.
   */
  public abstract boolean remove(MetricRegistryInfo key);

  /**
   * Returns the MetricRegistry if found.
   *
   * @param info the info for the registry.
   * @return a MetricRegistry optional.
   */
  public abstract Optional<RatlyMetricRegistry> get(MetricRegistryInfo info);

  /**
   * Returns MetricRegistryInfo's for the MetricRegistry's created.
   *
   * @return MetricRegistryInfo's for the MetricRegistry's created.
   */
  public abstract Set<MetricRegistryInfo> getMetricRegistryInfos();

  /**
   * Returns MetricRegistry's created.
   *
   * @return MetricRegistry's created.
   */
  public abstract Collection<RatlyMetricRegistry> getMetricRegistries();

  /**
   * Add hook to register reporter for the metricRegistry.
   *
   * @param reporterRegistration Consumer to create the reporter for the registry.
   * @param stopReporter Consumer to stop the reporter for the registry.
   */
  public abstract void addReporterRegistration(Consumer<RatlyMetricRegistry> reporterRegistration,
      Consumer<RatlyMetricRegistry> stopReporter);

  /**
   * Remove hook of reporter for the metricRegistry.
   *
   * @param reporterRegistration Consumer to create the reporter for the registry.
   * @param stopReporter Consumer to stop the reporter for the registry.
   */
  public void removeReporterRegistration(Consumer<RatlyMetricRegistry> reporterRegistration,
      Consumer<RatlyMetricRegistry> stopReporter) {
  }

  /**
   * Enable jmx reporter for the metricRegistry.
   */
  public abstract void enableJmxReporter();

  /**
   * Enable console reporter for the metricRegistry.
   *
   * @param consoleReportRate Console report rate.
   */
  public abstract void enableConsoleReporter(TimeDuration consoleReportRate);

}
