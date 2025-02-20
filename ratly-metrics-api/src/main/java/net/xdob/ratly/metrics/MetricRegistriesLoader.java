
package net.xdob.ratly.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import net.xdob.ratly.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class MetricRegistriesLoader {
  private static final Logger LOG = LoggerFactory.getLogger(MetricRegistriesLoader.class);

  static final String DEFAULT_CLASS = "net.xdob.ratly.metrics.impl.DefaultMetricRegistries";

  private MetricRegistriesLoader() {
  }

  /**
   * Creates a {@link MetricRegistries} instance using the corresponding {@link MetricRegistries}
   * available to {@link ServiceLoader} on the classpath. If no instance is found, then default
   * implementation will be loaded.
   * @return A {@link MetricRegistries} implementation.
   */
  public static MetricRegistries load() {
    List<MetricRegistries> availableImplementations = getDefinedImplementations();
    return load(availableImplementations);
  }

  /**
   * Creates a {@link MetricRegistries} instance using the corresponding {@link MetricRegistries}
   * available to {@link ServiceLoader} on the classpath. If no instance is found, then default
   * implementation will be loaded.
   * @return A {@link MetricRegistries} implementation.
   */
  @VisibleForTesting
  static MetricRegistries load(List<MetricRegistries> registries) {
    if (registries.isEmpty()) {
      try {
        return ReflectionUtils.newInstance(Class.forName(DEFAULT_CLASS).asSubclass(MetricRegistries.class));
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException("Failed to load default MetricRegistries " + DEFAULT_CLASS, e);
      }
    }

    final MetricRegistries first = registries.get(0);
    if (registries.size() == 1) {
      // One and only one instance -- what we want/expect
      LOG.debug("Loaded {}", first.getClass());
    } else {
      // Tell the user they're doing something wrong, and choose the first impl.
      final List<? extends Class<?>> classes = registries.stream().map(Object::getClass).collect(Collectors.toList());
      LOG.warn("Found multiple MetricRegistries: {}. Using the first: {}", classes, first.getClass());
    }
    return first;
  }

  private static List<MetricRegistries> getDefinedImplementations() {
    ServiceLoader<MetricRegistries> loader = ServiceLoader.load(
        MetricRegistries.class,
        MetricRegistries.class.getClassLoader());
    List<MetricRegistries> availableFactories = new ArrayList<>();
    for (MetricRegistries impl : loader) {
      availableFactories.add(impl);
    }
    return availableFactories;
  }
}
