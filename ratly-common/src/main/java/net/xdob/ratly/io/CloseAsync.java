
package net.xdob.ratly.io;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/** Support the {@link CloseAsync#closeAsync()} method. */
public interface CloseAsync<REPLY> extends AutoCloseable {
  /** Close asynchronously. */
  CompletableFuture<REPLY> closeAsync();

  /**
   * The same as {@link AutoCloseable#close()}.
   *
   * The default implementation simply calls {@link CloseAsync#closeAsync()}
   * and then waits for the returned future to complete.
   */
  default void close() throws Exception {
    try {
      closeAsync().get();
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      throw cause instanceof Exception? (Exception)cause: e;
    }
  }
}
