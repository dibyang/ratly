package net.xdob.ratly.util;

import java.util.*;
import java.util.function.Supplier;

public interface Preconditions {
  /**
   * Assert if the given value is true.
   * @param value the value to be asserted.
   * @throws IllegalStateException if the given value is false.
   */
  static void assertTrue(boolean value) {
    if (!value) {
      throw new IllegalStateException();
    }
  }

  /**
   * Assert if the given value is true.
   * @param value the value to be asserted.
   * @param message The exception message.
   * @throws IllegalStateException with the given message if the given value is false.
   */
  static void assertTrue(boolean value, Object message) {
    if (!value) {
      if (message instanceof Supplier) {
        message = ((Supplier<?>) message).get();
      }
      throw new IllegalStateException(String.valueOf(message));
    }
  }

  /**
   * Assert if the given value is true.
   * @param value the value to be asserted.
   * @param format exception message format.
   * @param args exception message arguments.
   * @throws IllegalStateException if the given value is false.
   * The exception message is constructed by {@link String#format(String, Object...)}
   * with the given format and arguments.
   */
  static void assertTrue(boolean value, String format, Object... args) {
    if (!value) {
      throw new IllegalStateException(String.format(format, args));
    }
  }

  /**
   * Assert if the given value is true.
   * @param value the value to be asserted.
   * @param message The exception message supplier.
   * @throws IllegalStateException with the given message if the given value is false.
   */
  static void assertTrue(boolean value, Supplier<Object> message) {
    if (!value) {
      throw new IllegalStateException(String.valueOf(message.get()));
    }
  }

  static void assertSame(int expected, int computed, String name) {
    assertTrue(expected == computed,
        () -> name + ": expected == " + expected + " but computed == " + computed);
  }

  static void assertSame(long expected, long computed, String name) {
    assertTrue(expected == computed,
        () -> name + ": expected == " + expected + " but computed == " + computed);
  }

  static void assertSame(Object expected, Object computed, String name) {
    assertTrue(expected == computed,
        () -> name + ": expected == " + expected + " but computed == " + computed);
  }

  static void assertEquals(Object expected, Object computed, String name) {
    assertTrue(Objects.equals(expected, computed),
        () -> name + ": expected == " + expected + " but computed == " + computed);
  }

  static void assertNull(Object object, Supplier<Object> message) {
    assertTrue(object == null, message);
  }

  static void assertNull(Object object, String name) {
    assertNull(object, () -> name + " is expected to be null but "
        + name + " = " + object + " != null, class = " + object.getClass());
  }

  static <T> T assertNotNull(T object, Supplier<Object> message) {
    assertTrue(object != null, message);
    return object;
  }

  static <T> T assertNotNull(T object, String name) {
    Preconditions.assertTrue(object != null, () -> name + " == null");
    return object;
  }

  static <T> T assertNotNull(T object, String format, Object... args) {
    assertTrue(object != null, format, args);
    return object;
  }

  static <T> T assertInstanceOf(Object object, Class<T> clazz) {
    assertTrue(clazz.isInstance(object),
        () -> "Required instance of " + clazz + " but object.getClass() is " + object.getClass());
    return clazz.cast(object);
  }

  static <K, V> void assertEmpty(Map<K, V> map, Object name) {
    assertTrue(map.isEmpty(), () -> "The " + name + " map is non-empty: " + map);
  }

  static <T> void assertUnique(Iterable<T> first) {
    assertUnique(first, Collections.emptyList());
  }

  static <T> void assertUnique(Iterable<T> original, Iterable<T> toBeAdded) {
    final Set<T> set = new HashSet<>();
    for(T t : original) {
      assertTrue(set.add(t), () -> "Found duplicated element " + t + " in " + original);
    }
    for(T t : toBeAdded) {
      assertTrue(set.add(t), () -> "Found duplicated element " + t + " when adding " + toBeAdded + " to " + original);
    }
  }
}
