package net.xdob.ratly.conf;

import net.xdob.ratly.security.TlsConf;
import com.google.common.base.Objects;
import net.xdob.ratly.util.NetUtils;
import net.xdob.ratly.util.SizeInBytes;
import net.xdob.ratly.util.TimeDuration;
import net.xdob.ratly.util.function.CheckedBiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 配置支持工具类
 */
public interface ConfUtils {
  Logger LOG = LoggerFactory.getLogger(ConfUtils.class);

  /**
   * 构建获取配置日志
   * @param key
   * @param value
   * @param defaultValue
   * @param logger
   * @param <T>
   */
  static <T> void log4get(String key, T value, T defaultValue, Consumer<String> logger) {
    if (logger != null) {
      logger.accept(String.format("%s = %s (%s)", key, value,
          Objects.equal(value, defaultValue)? "default": "custom"));
    }
  }

  /**
   * 构建使用回退配置日志
   * @param key
   * @param fallbackKey
   * @param fallbackValue
   * @param logger
   * @param <T>
   */
  static <T> void log4fallback(String key, String fallbackKey, T fallbackValue, Consumer<String> logger) {
    if (logger != null) {
      logger.accept(String.format("%s = %s (fallback to %s)", key, fallbackValue, fallbackKey));
    }
  }

  static void log4set(String key, Object value) {
    LOG.debug("set {} = {}", key, value);
  }

  static BiConsumer<String, Integer> requireMin(int min) {
    return (key, value) -> {
      if (value < min) {
        throw new IllegalArgumentException(
            key + " = " + value + " < min = " + min);
      }
    };
  }

  static BiConsumer<String, Integer> requireMax(int max) {
    return (key, value) -> {
      if (value > max) {
        throw new IllegalArgumentException(
            key + " = " + value + " > max = " + max);
      }
    };
  }

  static BiConsumer<String, Double> requireMin(double min) {
    return (key, value) -> {
      if (value < min) {
        throw new IllegalArgumentException(
            key + " = " + value + " < min = " + min);
      }
    };
  }

  static BiConsumer<String, Double> requireMax(double max) {
    return (key, value) -> {
      if (value > max) {
        throw new IllegalArgumentException(
            key + " = " + value + " > max = " + max);
      }
    };
  }

  static BiConsumer<String, Long> requireMin(SizeInBytes min) {
    return requireMin(min.getSize());
  }

  static BiConsumer<String, Long> requireMin(long min) {
    return (key, value) -> {
      if (value < min) {
        throw new IllegalArgumentException(
            key + " = " + value + " < min = " + min);
      }
    };
  }

  static BiConsumer<String, SizeInBytes> requireMinSizeInByte(SizeInBytes min) {
    return (key, value) -> {
      if (value.getSize() < min.getSize()) {
        throw new IllegalArgumentException(
            key + " = " + value + " < min = " + min);
      }
    };
  }

  static BiConsumer<String, Long> requireMax(long max) {
    return (key, value) -> {
      if (value > max) {
        throw new IllegalArgumentException(
            key + " = " + value + " > max = " + max);
      }
    };
  }

  static BiConsumer<String, TimeDuration> requireNonNegativeTimeDuration() {
    return (key, value) -> {
      if (value.isNegative()) {
        throw new IllegalArgumentException(
            key + " = " + value + " is negative.");
      }
    };
  }

  static BiConsumer<String, TimeDuration> requirePositive() {
    return (key, value) -> {
      if (value.getDuration() <= 0) {
        throw new IllegalArgumentException(
            key + " = " + value + " is non-positive.");
      }
    };
  }

  static BiFunction<String, Long, Integer> requireInt() {
    return (key, value) -> {
      try {
        return Math.toIntExact(value);
      } catch (ArithmeticException ae) {
        throw new IllegalArgumentException(
            "Failed to cast " + key + " = " + value + " to int.", ae);
      }
    };
  }

  @SafeVarargs
  static boolean getBoolean(
      BiFunction<String, Boolean, Boolean> booleanGetter,
      String key, boolean defaultValue, Consumer<String> logger, BiConsumer<String, Boolean>... assertions) {
    return get(booleanGetter, key, defaultValue, logger, assertions);
  }

  @SafeVarargs
  static int getInt(
      BiFunction<String, Integer, Integer> integerGetter,
      String key, int defaultValue, Consumer<String> logger, BiConsumer<String, Integer>... assertions) {
    return get(integerGetter, key, defaultValue, logger, assertions);
  }

  @SafeVarargs
  static int getInt(
      BiFunction<String, Integer, Integer> integerGetter,
      String key, int defaultValue, String fallbackKey, int fallbackValue,
      Consumer<String> logger, BiConsumer<String, Integer>... assertions) {
    return get(integerGetter, key, defaultValue, fallbackKey, fallbackValue, logger, assertions);
  }

  @SafeVarargs
  static long getLong(
      BiFunction<String, Long, Long> longGetter,
      String key, long defaultValue, Consumer<String> logger, BiConsumer<String, Long>... assertions) {
    return get(longGetter, key, defaultValue, logger, assertions);
  }

  @SafeVarargs
  static double getDouble(
      BiFunction<String, Double, Double> doubleGetter,
      String key, double defaultValue, Consumer<String> logger, BiConsumer<String, Double>... assertions) {
    return get(doubleGetter, key, defaultValue, logger, assertions);
  }

  @SafeVarargs
  static File getFile(
      BiFunction<String, File, File> fileGetter,
      String key, File defaultValue, Consumer<String> logger, BiConsumer<String, File>... assertions) {
    return get(fileGetter, key, defaultValue, logger, assertions);
  }

  @SafeVarargs
  static List<File> getFiles(
      BiFunction<String, List<File>, List<File>> fileGetter,
      String key, List<File> defaultValue, Consumer<String> logger, BiConsumer<String, List<File>>... assertions) {
    return get(fileGetter, key, defaultValue, logger, assertions);
  }


  @SafeVarargs
  static SizeInBytes getSizeInBytes(
      BiFunction<String, SizeInBytes, SizeInBytes> getter,
      String key, SizeInBytes defaultValue, Consumer<String> logger, BiConsumer<String, SizeInBytes>... assertions) {
    final SizeInBytes value = get(getter, key, defaultValue, logger, assertions);
    requireMin(0L).accept(key, value.getSize());
    return value;
  }

  @SafeVarargs
  static TimeDuration getTimeDuration(
      BiFunction<String, TimeDuration, TimeDuration> getter,
      String key, TimeDuration defaultValue, Consumer<String> logger, BiConsumer<String, TimeDuration>... assertions) {
    final TimeDuration value = get(getter, key, defaultValue, logger, assertions);
    requireNonNegativeTimeDuration().accept(key, value);
    return value;
  }

  @SafeVarargs
  static TimeDuration getTimeDuration(
        BiFunction<String, TimeDuration, TimeDuration> getter,
        String key, TimeDuration defaultValue, String fallbackKey, TimeDuration fallbackValue,
        Consumer<String> logger, BiConsumer<String, TimeDuration>... assertions) {
    final TimeDuration value = get(getter, key, defaultValue, fallbackKey, fallbackValue, logger, assertions);
    requireNonNegativeTimeDuration().accept(key, value);
    return value;
  }


  static TlsConf getTlsConf(
      Function<String, TlsConf> tlsConfGetter,
      String key, Consumer<String> logger) {
    return get((k, d) -> tlsConfGetter.apply(k), key, null, logger);
  }

  @SafeVarargs
  static <T> T get(BiFunction<String, T, T> getter,
      String key, T defaultValue, Consumer<String> logger, BiConsumer<String, T>... assertions) {
    final T value = getter.apply(key, defaultValue);
    log4get(key, value, defaultValue, logger);
    Arrays.asList(assertions).forEach(a -> a.accept(key, value));
    return value;
  }

  @SafeVarargs
  static <T> T get(BiFunction<String, T, T> getter,
      String key, T defaultValue, String fallbackKey, T fallbackValue,
      Consumer<String> logger, BiConsumer<String, T>... assertions) {
    T value = get(getter, key, defaultValue, null, assertions);
    if (value != defaultValue) {
      log4get(key, value, defaultValue, logger);
      return value;
    } else {
      log4fallback(key, fallbackKey, fallbackValue, logger);
      return fallbackValue;
    }
  }

  static InetSocketAddress getInetSocketAddress(
      BiFunction<String, String, String> stringGetter,
      String key, String defaultValue, Consumer<String> logger) {
    return NetUtils.createSocketAddr(get(stringGetter, key, defaultValue, logger));
  }

  @SafeVarargs
  static void setBoolean(
      BiConsumer<String, Boolean> booleanSetter, String key, boolean value,
      BiConsumer<String, Boolean>... assertions) {
    set(booleanSetter, key, value, assertions);
  }

  @SafeVarargs
  static void setInt(
      BiConsumer<String, Integer> integerSetter, String key, int value,
      BiConsumer<String, Integer>... assertions) {
    set(integerSetter, key, value, assertions);
  }

  @SafeVarargs
  static void setLong(
      BiConsumer<String, Long> longSetter, String key, long value,
      BiConsumer<String, Long>... assertions) {
    set(longSetter, key, value, assertions);
  }

  @SafeVarargs
  static void setDouble(
      BiConsumer<String, Double> doubleSetter, String key, double value,
      BiConsumer<String, Double>... assertions) {
    set(doubleSetter, key, value, assertions);
  }

  @SafeVarargs
  static void setFile(
      BiConsumer<String, File> fileSetter, String key, File value,
      BiConsumer<String, File>... assertions) {
    set(fileSetter, key, value, assertions);
  }

  @SafeVarargs
  static void setFiles(
      BiConsumer<String, List<File>> fileSetter, String key, List<File> value,
      BiConsumer<String, List<File>>... assertions) {
    set(fileSetter, key, value, assertions);
  }

  @SafeVarargs
  static void setSizeInBytes(
      BiConsumer<String, String> stringSetter, String key, SizeInBytes value,
      BiConsumer<String, Long>... assertions) {
    final long v = value.getSize();
    Arrays.asList(assertions).forEach(a -> a.accept(key, v));
    set(stringSetter, key, value.getInput());
  }

  @SafeVarargs
  static void setTimeDuration(
      BiConsumer<String, TimeDuration> timeDurationSetter, String key, TimeDuration value,
      BiConsumer<String, TimeDuration>... assertions) {
    set(timeDurationSetter, key, value, assertions);
  }

  static void setTlsConf(
      BiConsumer<String, TlsConf> tlsConfSetter, String key, TlsConf value) {
    set(tlsConfSetter, key, value);
  }

  @SafeVarargs
  static <T> void set(
      BiConsumer<String, T> setter, String key, T value,
      BiConsumer<String, T>... assertions) {
    Arrays.asList(assertions).forEach(a -> a.accept(key, value));
    setter.accept(key, value);
    log4set(key, value);
  }

  static void printAll(Class<?> confClass) {
    ConfUtils.printAll(confClass, System.out::println);
  }

  static void printAll(Class<?> confClass, Consumer<Object> out) {
    if (confClass.isEnum()) {
      return;
    }
    out.accept("");
    out.accept("******* " + confClass + " *******");
    Arrays.asList(confClass.getDeclaredFields())
        .forEach(f -> printField(confClass, out, f));
    Arrays.asList(confClass.getClasses())
        .forEach(c -> printAll(c, s -> out.accept("  " + s)));
  }

  static void printField(Class<?> confClass, Consumer<Object> out, Field f) {
    final int modifiers = f.getModifiers();
    if (!Modifier.isStatic(modifiers)) {
      throw new IllegalStateException("Found non-static field " + f);
    }
    if (!Modifier.isFinal(modifiers)) {
      throw new IllegalStateException("Found non-final field " + f);
    }
    if (printKey(confClass, out, f, "KEY", "DEFAULT", ConfUtils::append)) {
      return;
    }
    if (printKey(confClass, out, f, "PARAMETER", "CLASS",
        (b, classField) -> b.append(classField.get(null)))) {
      return;
    }
    final String fieldName = f.getName();
    if ("LOG".equals(fieldName) || "$jacocoData".equals(fieldName)) {
      return;
    }
    if (!"PREFIX".equals(fieldName)) {
      throw new IllegalStateException("Unexpected field: " + fieldName);
    }
    try {
      out.accept("constant: " + fieldName + " = " + f.get(null));
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("Failed to access " + f, e);
    }
  }

  static void append(StringBuilder b, Field defaultField) throws IllegalAccessException {
    b.append(defaultField.getGenericType().getTypeName());

    final Class<?> type = defaultField.getType();
    if (type.isEnum()) {
      b.append(" enum[");
      for(Object e : defaultField.getType().getEnumConstants()) {
        b.append(e).append(", ");
      }
      b.setLength(b.length() - 2);
      b.append("]");
    }

    b.append(", ").append("default=").append(defaultField.get(null));
  }

  static boolean printKey(
      Class<?> confClass, Consumer<Object> out, Field f, String key, String defaultName,
      CheckedBiConsumer<StringBuilder, Field, IllegalAccessException> processDefault) {
    final String fieldName = f.getName();
    if (fieldName.endsWith("_" + defaultName)) {
      return true;
    }
    if (!fieldName.endsWith("_" + key)) {
      return false;
    }
    final StringBuilder b = new StringBuilder();
    final Object keyName;
    try {
      keyName = f.get(null);
      b.append(key.toLowerCase()).append(": ").append(keyName);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("Failed to access " + fieldName, e);
    }
    assertKey(fieldName, key.length(), keyName, confClass);
    final String defaultFieldName = fieldName.substring(0, fieldName.length() - key.length()) + defaultName;
    b.append(" (");
    try {
      final Field defaultField = confClass.getDeclaredField(defaultFieldName);
      processDefault.accept(b, defaultField);
    } catch (NoSuchFieldException e) {
      throw new IllegalStateException(defaultName + " not found for field " + f, e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("Failed to access " + defaultFieldName, e);
    }
    b.append(")");

    out.accept(b);
    return true;
  }

  static String normalizeName(String name) {
    return name.replaceAll("[._-]", "").toLowerCase();
  }

  static void assertKey(String fieldName, int toTruncate, Object keyName, Class<?> confClass) {
    final String normalizedFieldName = normalizeName(fieldName.substring(0, fieldName.length() - toTruncate));
    final String normalizedKeyName = normalizeName("" + keyName);

    if (!normalizedKeyName.endsWith(normalizedFieldName)) {
      throw new IllegalStateException("Field and key mismatched: fieldName = " + fieldName + " (" + normalizedFieldName
          + ") but keyName = " + keyName + " (" + normalizedKeyName + ")");
    }

    // check getter and setter methods
    boolean getter = false;
    boolean setter = false;
    for(Method m : confClass.getMethods()) {
      final String name = m.getName();
      if (name.equalsIgnoreCase(normalizedFieldName)) {
        getter = true;
      }
      if (name.equalsIgnoreCase("set" + normalizedFieldName)) {
        setter = true;
      }
    }
    if (!getter) {
      throw new IllegalStateException("Getter method not found for " + fieldName);
    }
    if (!setter) {
      throw new IllegalStateException("Setter method not found for " + fieldName);
    }
  }
}
