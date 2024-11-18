
package net.xdob.ratly.metrics.dropwizard3;

import net.xdob.ratly.metrics.MetricRegistries;
import net.xdob.ratly.metrics.MetricRegistryInfo;
import net.xdob.ratly.metrics.RatlyMetricRegistry;
import net.xdob.ratly.util.TimeDuration;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;

/**
 * Helper class to add JVM metrics.
 */
public interface Dm3JvmMetrics {
  static void initJvmMetrics(TimeDuration consoleReportRate) {
    final MetricRegistries registries = MetricRegistries.global();
    Dm3JvmMetrics.addJvmMetrics(registries);
    registries.enableConsoleReporter(consoleReportRate);
    registries.enableJmxReporter();
  }

  static void addJvmMetrics(MetricRegistries registries) {
    MetricRegistryInfo info = new MetricRegistryInfo("jvm", "ratly_jvm", "jvm", "jvm metrics");

    RatlyMetricRegistry registry = registries.create(info);

    final Dm3RatlyMetricRegistryImpl impl = Dm3RatlyMetricRegistryImpl.cast(registry);
    impl.registerAll("gc", new GarbageCollectorMetricSet());
    impl.registerAll("memory", new MemoryUsageGaugeSet());
    impl.registerAll("threads", new ThreadStatesGaugeSet());
    impl.registerAll("classLoading", new ClassLoadingGaugeSet());
  }
}
