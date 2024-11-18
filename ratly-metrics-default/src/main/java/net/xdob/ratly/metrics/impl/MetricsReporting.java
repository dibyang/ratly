
package net.xdob.ratly.metrics.impl;

import net.xdob.ratly.metrics.RatlyMetricRegistry;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.jmx.JmxReporter;
import net.xdob.ratly.util.TimeDuration;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

final class MetricsReporting {
  private MetricsReporting() {
  }

  static Consumer<RatlyMetricRegistry> consoleReporter(TimeDuration rate) {
    return registry -> consoleReporter(rate, registry);
  }

  private static void consoleReporter(TimeDuration rate, RatlyMetricRegistry registry) {
    final RatlyMetricRegistryImpl impl = RatlyMetricRegistryImpl.cast(registry);
    final ConsoleReporter reporter = ConsoleReporter.forRegistry(impl.getDropWizardMetricRegistry())
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build();
    reporter.start(rate.getDuration(), rate.getUnit());
    impl.setConsoleReporter(reporter);
  }

  static Consumer<RatlyMetricRegistry> stopConsoleReporter() {
    return MetricsReporting::stopConsoleReporter;
  }

  private static void stopConsoleReporter(RatlyMetricRegistry registry) {
    final RatlyMetricRegistryImpl impl = RatlyMetricRegistryImpl.cast(registry);
    Optional.ofNullable(impl.getConsoleReporter()).ifPresent(ScheduledReporter::close);
  }

  static Consumer<RatlyMetricRegistry> jmxReporter() {
    return MetricsReporting::jmxReporter;
  }

  private static void jmxReporter(RatlyMetricRegistry registry) {
    final RatlyMetricRegistryImpl impl = RatlyMetricRegistryImpl.cast(registry);
    final JmxReporter reporter = JmxReporter.forRegistry(impl.getDropWizardMetricRegistry())
        .inDomain(registry.getMetricRegistryInfo().getApplicationName())
        .createsObjectNamesWith(new RatlyObjectNameFactory())
        .build();
    reporter.start();
    impl.setJmxReporter(reporter);
  }


  static Consumer<RatlyMetricRegistry> stopJmxReporter() {
    return MetricsReporting::stopJmxReporter;
  }

  private static void stopJmxReporter(RatlyMetricRegistry registry) {
    final RatlyMetricRegistryImpl impl = RatlyMetricRegistryImpl.cast(registry);
    Optional.ofNullable(impl.getJmxReporter()).ifPresent(JmxReporter::close);
  }
}
