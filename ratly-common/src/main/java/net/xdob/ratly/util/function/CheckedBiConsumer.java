
package net.xdob.ratly.util.function;

/** BiConsumer with a throws-clause. */
@FunctionalInterface
public interface CheckedBiConsumer<LEFT, RIGHT, THROWABLE extends Throwable> {
  /**
   * The same as {@link java.util.function.BiConsumer#accept(Object, Object)}
   * except that this method is declared with a throws-clause.
   */
  void accept(LEFT left, RIGHT right) throws THROWABLE;
}
