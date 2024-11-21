package net.xdob.ratly.conf;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 泛型参数map.
 * 支持存储任意对象
 * <p>
 * 不支持空的键和值
 * <p>
 * 这个类是线程安全的
 */
public class Parameters {
  private final Map<String, Object> map = new ConcurrentHashMap<>();

  /** Put the key-value pair to the map. */
  public <T> T put(String key, T value, Class<T> valueClass) {
    return valueClass.cast(map.put(
        Objects.requireNonNull(key, "key is null"),
        Objects.requireNonNull(value, () -> "value is null, key=" + key)));
  }

  /**
   * @param <T> The value type.
   * @return The value mapped to the given key;
   *         or return null if the key does not map to any value.
   * @throws IllegalArgumentException if the mapped value is not an instance of the given class.
   */
  public <T> T get(String key, Class<T> valueClass) {
    final Object value = map.get(Objects.requireNonNull(key, "key is null"));
    return valueClass.cast(value);
  }

  /**
   * The same as {@link #get(String, Class)} except that this method throws
   * a {@link NullPointerException} if the key does not map to any value.
   */
  public <T> T getNonNull(String key, Class<T> valueClass) {
    final T value = get(key, valueClass);
    if (value != null) {
      return value;
    }
    throw new NullPointerException("The key " + key + " does not map to any value.");
  }
}
