
package net.xdob.ratly.util.function;

/** BiFunction with a throws-clause. */
@FunctionalInterface
public interface CheckedBiFunction<LEFT, RIGHT, OUTPUT, THROWABLE extends Throwable> {
  /**
   * The same as {@link java.util.function.BiFunction#apply(Object, Object)}
   * except that this method is declared with a throws-clause.
   */
  OUTPUT apply(LEFT left, RIGHT right) throws THROWABLE;
}
