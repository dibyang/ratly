package net.xdob.ratly.util.function;

/** Runnable with a throws-clause. */
@FunctionalInterface
public interface CheckedRunnable<THROWABLE extends Throwable> {
  /**
   * The same as {@link Runnable#run()}
   * except that this method is declared with a throws-clause.
   */
  void run() throws THROWABLE;

  static <THROWABLE extends Throwable> CheckedSupplier<?, THROWABLE> asCheckedSupplier(
      CheckedRunnable<THROWABLE> runnable) {
    return () -> {
      runnable.run();
      return null;
    };
  }
}