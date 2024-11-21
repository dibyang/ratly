package net.xdob.ratly.util.function;

/** Supplier with a throws-clause. */
@FunctionalInterface
public interface CheckedSupplier<OUTPUT, THROWABLE extends Throwable> {
  /**
   * The same as {@link java.util.function.Supplier#get()}
   * except that this method is declared with a throws-clause.
   */
  OUTPUT get() throws THROWABLE;
}
