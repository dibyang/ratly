package net.xdob.ratly.util.function;

/** Function with a throws-clause. */
@FunctionalInterface
public interface CheckedFunction<INPUT, OUTPUT, THROWABLE extends Throwable> {
  /**
   * The same as {@link java.util.function.Function#apply(Object)}
   * except that this method is declared with a throws-clause.
   */
  OUTPUT apply(INPUT input) throws THROWABLE;
}
