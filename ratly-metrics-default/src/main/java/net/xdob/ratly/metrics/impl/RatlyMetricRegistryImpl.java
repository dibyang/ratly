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
 * 自定义实现的 {@link MetricRegistry}。
 */
public class RatlyMetricRegistryImpl implements RatlyMetricRegistry, DropWizardMetricSupport {


  private final MetricRegistry metricRegistry = new MetricRegistry();

  private final MetricRegistryInfo info;
  private final String namePrefix;
  private final Map<String, String> metricNameCache = new ConcurrentHashMap<>();

  private JmxReporter jmxReporter;
  private ConsoleReporter consoleReporter;

  // 构造函数，初始化指标注册表
  public RatlyMetricRegistryImpl(MetricRegistryInfo info) {
    this.info = Objects.requireNonNull(info, "info == null");
    this.namePrefix = MetricRegistry.name(info.getApplicationName(), info.getMetricsComponentName(), info.getPrefix());
  }

  // 返回指定指标名称的 Timekeeper
  @Override
  public Timekeeper timer(String name) {
    return new DefaultTimekeeperImpl(metricRegistry.timer(getMetricName(name)));
  }

  // 将 Dropwizard Counter 转换为 LongCounter
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

  // 返回指定指标名称的 LongCounter
  @Override
  public LongCounter counter(String name) {
    return toLongCounter(metricRegistry.counter(getMetricName(name)));
  }

  // 移除指定名称的指标
  @Override
  public boolean remove(String name) {
    return metricRegistry.remove(getMetricName(name));
  }

  // 将 supplier 转换为 Gauge
  static <T> Gauge<T> toGauge(Supplier<T> supplier) {
    return supplier::get;
  }

  // 注册指定名称和 supplier 的 gauge
  @Override
  public <T> void gauge(String name, Supplier<Supplier<T>> gaugeSupplier) {
    metricRegistry.gauge(getMetricName(name), () -> toGauge(gaugeSupplier.get()));
  }

  // 返回匹配给定过滤器的 gauge 的排序映射
  public SortedMap<String, Gauge> getGauges(MetricFilter filter) {
    return metricRegistry.getGauges(filter);
  }

  // 根据短名称检索指标
  @VisibleForTesting
  public Metric get(String shortName) {
    return metricRegistry.getMetrics().get(getMetricName(shortName));
  }

  // 根据短名称生成完整的指标名称
  private String getMetricName(String shortName) {
    return metricNameCache.computeIfAbsent(shortName, key -> MetricRegistry.name(namePrefix, shortName));
  }

  // 注册指定名称的指标
  private <T extends Metric> T register(String name, T metric) throws IllegalArgumentException {
    return metricRegistry.register(getMetricName(name), metric);
  }



  // 返回与此注册表关联的 MetricRegistryInfo
  @Override public MetricRegistryInfo getMetricRegistryInfo(){
    return this.info;
  }


  public void registerAll(String prefix, MetricSet metricSet) {
    for (Map.Entry<String, Metric> entry : metricSet.getMetrics().entrySet()) {
      if (entry.getValue() instanceof MetricSet) {
        registerAll(prefix + "." + entry.getKey(), (MetricSet) entry.getValue());
      } else {
        register(prefix + "." + entry.getKey(), entry.getValue());
      }
    }
  }

  // 返回底层的 Dropwizard MetricRegistry
  @Override
  public MetricRegistry getDropWizardMetricRegistry() {
    return metricRegistry;
  }

  // 设置此注册表的 JMX 报告器
  @Override
  public void setJmxReporter(JmxReporter jmxReporter) {
    this.jmxReporter = jmxReporter;
  }

  // 返回此注册表的 JMX 报告器
  @Override
  public JmxReporter getJmxReporter() {
    return this.jmxReporter;
  }

  // 设置此注册表的控制台报告器
  @Override
  public void setConsoleReporter(ConsoleReporter consoleReporter) {
    this.consoleReporter = consoleReporter;
  }

  // 返回此注册表的控制台报告器
  @Override
  public ConsoleReporter getConsoleReporter() {
    return this.consoleReporter;
  }
}