

package net.xdob.ratly.metrics;

import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.MemoizedSupplier;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * 该类保存了度量指标的名称、描述以及与JMX相关的上下文名称。
 * <p>
 * 该类是不可变的。
 */
public class MetricRegistryInfo {
  private final String prefix;
  private final String metricsDescription;
  private final String metricsComponentName;
  private final String applicationName;

  private final Supplier<Integer> hash = MemoizedSupplier.valueOf(this::computeHash);

  /**
   * 构造一个新的MetricRegistryInfo对象。
   *
   * @param prefix            类名或组件名，该度量注册表收集度量指标的对象
   * @param applicationName   应用程序名称，需要小写，因为用于Hadoop2Metrics
   * @param metricsComponentName 组件名称，需要小写，因为用于Hadoop2Metrics
   * @param metricsDescription 该注册表收集的度量指标的描述
   */
  public MetricRegistryInfo(String prefix, String applicationName, String metricsComponentName,
      String metricsDescription) {
    this.prefix = prefix;
    this.applicationName = applicationName;
    this.metricsComponentName = metricsComponentName;
    this.metricsDescription = metricsDescription;
  }

  public String getApplicationName() {
    return this.applicationName;
  }

  /**
   * 获取正在收集度量指标的组件名称。
   *
   * @return 正在收集度量指标的组件名称
   */
  public String getMetricsComponentName() {
    return metricsComponentName;
  }

  /**
   * 获取该度量注册表暴露的内容的描述。
   *
   * @return 该注册表收集的度量指标的描述
   */
  public String getMetricsDescription() {
    return metricsDescription;
  }

  /**
   * 获取该注册表导出的度量指标的唯一前缀。
   *
   * @return 唯一前缀
   */
  public String getPrefix() {
    return prefix;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (!(obj instanceof MetricRegistryInfo)) {
      return false;
    }
    final MetricRegistryInfo that = (MetricRegistryInfo) obj;
    return Objects.equals(prefix, that.prefix)
        && Objects.equals(metricsDescription, that.metricsDescription)
        && Objects.equals(metricsComponentName, that.metricsComponentName)
        && Objects.equals(applicationName, that.applicationName);
  }

  @Override
  public int hashCode() {
    return hash.get();
  }

  private Integer computeHash() {
    return Objects.hash(prefix, metricsDescription, metricsComponentName);
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass())
        + ": applicationName=" + getApplicationName()
        + ", metricsComponentName=" + getMetricsComponentName()
        + ", prefix=" + getPrefix()
        + ", metricsDescription=" + getMetricsDescription();
  }
}
