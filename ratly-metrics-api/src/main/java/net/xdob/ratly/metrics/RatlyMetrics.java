
package net.xdob.ratly.metrics;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RatlyMetrics {
  static final Logger LOG = LoggerFactory.getLogger(RatlyMetrics.class);
  public static final String RATLY_APPLICATION_NAME_METRICS = "ratly";

  public static String getHeartbeatSuffix(boolean heartbeat) {
    return heartbeat ? "_heartbeat" : "";
  }

  private static <T> Function<Boolean, T> newHeartbeatFunction(String prefix, Function<String, T> function) {
    final T trueValue = function.apply(prefix + getHeartbeatSuffix(true));
    final T falseValue = function.apply(prefix + getHeartbeatSuffix(false));
    return b -> b? trueValue: falseValue;
  }

  protected static <T extends Enum<T>> Map<T, Map<String, LongCounter>> newCounterMaps(Class<T> clazz) {
    final EnumMap<T,Map<String, LongCounter>> maps = new EnumMap<>(clazz);
    Arrays.stream(clazz.getEnumConstants()).forEach(t -> maps.put(t, new ConcurrentHashMap<>()));
    return Collections.unmodifiableMap(maps);
  }

  protected static <T extends Enum<T>> Map<T, LongCounter> newCounterMap(
      Class<T> clazz, Function<T, LongCounter> constructor) {
    final EnumMap<T, LongCounter> map = new EnumMap<>(clazz);
    Arrays.stream(clazz.getEnumConstants()).forEach(t -> map.put(t, constructor.apply(t)));
    return Collections.unmodifiableMap(map);
  }

  protected static <T extends Enum<T>> Map<T, Timekeeper> newTimerMap(
      Class<T> clazz, Function<T, Timekeeper> constructor) {
    final EnumMap<T, Timekeeper> map = new EnumMap<>(clazz);
    Arrays.stream(clazz.getEnumConstants()).forEach(t -> map.put(t, constructor.apply(t)));
    return Collections.unmodifiableMap(map);
  }

  protected static RatlyMetricRegistry create(MetricRegistryInfo info) {
    Optional<RatlyMetricRegistry> metricRegistry = MetricRegistries.global().get(info);
    return metricRegistry.orElseGet(() -> {
      LOG.debug("Creating {}", info);
      return MetricRegistries.global().create(info);
    });
  }

  private final RatlyMetricRegistry registry;

  protected RatlyMetrics(RatlyMetricRegistry registry) {
    this.registry = registry;
  }

  public void unregister() {
    MetricRegistryInfo info = registry.getMetricRegistryInfo();
    LOG.debug("Unregistering {}", info);
    Optional<RatlyMetricRegistry> metricRegistry = MetricRegistries.global().get(info);
    if (metricRegistry.isPresent()) {
      MetricRegistries.global().remove(info);
    }
  }

  public final RatlyMetricRegistry getRegistry() {
    return registry;
  }

  protected Function<Boolean, Timekeeper> newHeartbeatTimer(String prefix) {
    return newHeartbeatFunction(prefix, getRegistry()::timer);
  }

  protected Function<Boolean, LongCounter> newHeartbeatCounter(String prefix) {
    return newHeartbeatFunction(prefix, getRegistry()::counter);
  }
}
