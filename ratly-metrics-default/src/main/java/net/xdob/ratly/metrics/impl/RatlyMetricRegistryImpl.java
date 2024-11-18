
package net.xdob.ratly.metrics.impl;

import net.xdob.ratly.metrics.LongCounter;
import net.xdob.ratly.metrics.MetricRegistryInfo;
import net.xdob.ratly.metrics.RatlyMetricRegistry;
import net.xdob.ratly.metrics.Timekeeper;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.jmx.JmxReporter;
import com.google.common.annotations.VisibleForTesting;

import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Custom implementation of {@link MetricRegistry}.
 */
public class RatlyMetricRegistryImpl implements RatlyMetricRegistry {
  static RatlyMetricRegistryImpl cast(RatlyMetricRegistry registry) {
    if (!(registry instanceof RatlyMetricRegistryImpl)) {
      throw new IllegalStateException("Unexpected class: " + registry.getClass().getName());
    }
    return (RatlyMetricRegistryImpl) registry;
  }

  private final MetricRegistry metricRegistry = new MetricRegistry();

  private final MetricRegistryInfo info;
  private final String namePrefix;
  private final Map<String, String> metricNameCache = new ConcurrentHashMap<>();

  private JmxReporter jmxReporter;
  private ConsoleReporter consoleReporter;

  public RatlyMetricRegistryImpl(MetricRegistryInfo info) {
    this.info = Objects.requireNonNull(info, "info == null");
    this.namePrefix = MetricRegistry.name(info.getApplicationName(), info.getMetricsComponentName(), info.getPrefix());
  }

  @Override
  public Timekeeper timer(String name) {
    return new DefaultTimekeeperImpl(metricRegistry.timer(getMetricName(name)));
  }

  static LongCounter toLongCounter(Counter c) {
    return new LongCounter() {
      @Override
      public void inc(long n) {
        c.inc(n);
      }

      @Override
      public void dec(long n) {
        c.dec(n);
      }

      @Override
      public long getCount() {
        return c.getCount();
      }
    };
  }

  @Override
  public LongCounter counter(String name) {
    return toLongCounter(metricRegistry.counter(getMetricName(name)));
  }

  @Override
  public boolean remove(String name) {
    return metricRegistry.remove(getMetricName(name));
  }

  static <T> Gauge<T> toGauge(Supplier<T> supplier) {
    return supplier::get;
  }

  @Override
  public <T> void gauge(String name, Supplier<Supplier<T>> gaugeSupplier) {
    metricRegistry.gauge(getMetricName(name), () -> toGauge(gaugeSupplier.get()));
  }

  public SortedMap<String, Gauge> getGauges(MetricFilter filter) {
    return metricRegistry.getGauges(filter);
  }

  @VisibleForTesting
  public Metric get(String shortName) {
    return metricRegistry.getMetrics().get(getMetricName(shortName));
  }

  private String getMetricName(String shortName) {
    return metricNameCache.computeIfAbsent(shortName, key -> MetricRegistry.name(namePrefix, shortName));
  }

  private <T extends Metric> T register(String name, T metric) throws IllegalArgumentException {
    return metricRegistry.register(getMetricName(name), metric);
  }


  public MetricRegistry getDropWizardMetricRegistry() {
    return metricRegistry;
  }

  @Override public MetricRegistryInfo getMetricRegistryInfo(){
    return this.info;
  }

  void registerAll(String prefix, MetricSet metricSet) {
    for (Map.Entry<String, Metric> entry : metricSet.getMetrics().entrySet()) {
      if (entry.getValue() instanceof MetricSet) {
        registerAll(prefix + "." + entry.getKey(), (MetricSet) entry.getValue());
      } else {
        register(prefix + "." + entry.getKey(), entry.getValue());
      }
    }
  }

  void setJmxReporter(JmxReporter jmxReporter) {
    this.jmxReporter = jmxReporter;
  }

  JmxReporter getJmxReporter() {
    return this.jmxReporter;
  }

  void setConsoleReporter(ConsoleReporter consoleReporter) {
    this.consoleReporter = consoleReporter;
  }

  ConsoleReporter getConsoleReporter() {
    return this.consoleReporter;
  }
}
