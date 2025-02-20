package net.xdob.ratly.metrics.impl;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.jmx.JmxReporter;

public interface DropWizardMetricSupport {
  // 使用指定前缀注册给定 MetricSet 中的所有指标
  void registerAll(String prefix, MetricSet metricSet);

  // 返回底层的 Dropwizard MetricRegistry
  MetricRegistry getDropWizardMetricRegistry();

  // 设置此注册表的 JMX 报告器
  void setJmxReporter(JmxReporter jmxReporter);

  // 返回此注册表的 JMX 报告器
  JmxReporter getJmxReporter();

  // 设置此注册表的控制台报告器
  void setConsoleReporter(ConsoleReporter consoleReporter);

  // 返回此注册表的控制台报告器
  ConsoleReporter getConsoleReporter();
}
