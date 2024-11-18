
package net.xdob.ratly.util.function;

/** Consumer with three input parameters. */
@FunctionalInterface
public interface TriConsumer<T, U, V> {
  /**
   * The same as {@link java.util.function.BiConsumer#accept(Object, Object)}}
   * except that this method is declared with three parameters.
   */
  void accept(T t, U u, V v);
}
