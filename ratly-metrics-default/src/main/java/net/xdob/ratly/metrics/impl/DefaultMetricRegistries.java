package net.xdob.ratly.metrics.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import net.xdob.ratly.metrics.MetricRegistries;
import net.xdob.ratly.metrics.MetricRegistryFactory;
import net.xdob.ratly.metrics.MetricRegistryInfo;
import net.xdob.ratly.metrics.RatlyMetricRegistry;
import net.xdob.ratly.util.RefCountingMap;
import net.xdob.ratly.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of MetricRegistries that does ref-counting.
 */
public class DefaultMetricRegistries extends MetricRegistries {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetricRegistries.class);
  public static final String NAME = "default";

  private final List<Consumer<RatlyMetricRegistry>> reporterRegistrations = new CopyOnWriteArrayList<>();

  private final List<Consumer<RatlyMetricRegistry>> stopReporters = new CopyOnWriteArrayList<>();

  private final MetricRegistryFactory factory;

  private final RefCountingMap<MetricRegistryInfo, RatlyMetricRegistry> registries;
  private final Object registerLock = new Object();

  public DefaultMetricRegistries() {
    this(new MetricRegistryFactoryImpl());
  }

  DefaultMetricRegistries(MetricRegistryFactory factory) {
    this.factory = factory;
    this.registries = new RefCountingMap<>();
  }

  @Override
  public RatlyMetricRegistry create(MetricRegistryInfo info) {
    return registries.put(info, () -> {
      if (reporterRegistrations.isEmpty()) {
        synchronized (registerLock) {
          if (reporterRegistrations.isEmpty()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("First MetricRegistry has been created without registering reporters. " +
                  "Hence registering JMX reporter by default.");
            }
            enableJmxReporter();
          }
        }
      }

      RatlyMetricRegistry registry = factory.create(info);
      reporterRegistrations.forEach(reg -> reg.accept(registry));
      return registry;
    });
  }

  @Override
  public boolean remove(MetricRegistryInfo key) {
    RatlyMetricRegistry registry = registries.get(key);
    if (registry != null) {
      stopReporters.forEach(reg -> reg.accept(registry));
    }

    return registries.remove(key) == null;
  }

  @Override
  public Optional<RatlyMetricRegistry> get(MetricRegistryInfo info) {
    return Optional.ofNullable(registries.get(info));
  }

  @Override
  public Collection<RatlyMetricRegistry> getMetricRegistries() {
    return Collections.unmodifiableCollection(registries.values());
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void clear() {
    registries.clear();
  }

  @Override
  public Set<MetricRegistryInfo> getMetricRegistryInfos() {
    return Collections.unmodifiableSet(registries.keySet());
  }

  @Override
  public void addReporterRegistration(Consumer<RatlyMetricRegistry> reporterRegistration,
      Consumer<RatlyMetricRegistry> stopReporter) {
    if (registries.size() > 0) {
      LOG.warn("New reporters are added after registries were created. Some metrics will be missing from the reporter. "
          + "Please add reporter before adding any new registry.");
    }
    this.reporterRegistrations.add(reporterRegistration);
    this.stopReporters.add(stopReporter);
  }

  @Override
  public void removeReporterRegistration(Consumer<RatlyMetricRegistry> reporterRegistration,
      Consumer<RatlyMetricRegistry> stopReporter) {
    this.reporterRegistrations.remove(reporterRegistration);
    this.stopReporters.remove(stopReporter);
  }

  @Override
  public void enableJmxReporter() {
    addReporterRegistration(
        MetricsReporting.jmxReporter(),
        MetricsReporting.stopJmxReporter());
  }

  @Override
  public void enableConsoleReporter(TimeDuration consoleReportRate) {
    addReporterRegistration(
        MetricsReporting.consoleReporter(consoleReportRate),
        MetricsReporting.stopConsoleReporter());
  }
}
