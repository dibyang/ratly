
package net.xdob.ratly.io;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;



/**
 * 异步关闭支持接口，close方法会将异步关闭变成同步关闭
 * Support the {@link CloseAsync#closeAsync()} method.
 * <p>
 * @param <REPLY>
 */
public interface CloseAsync<REPLY> extends AutoCloseable {
  /**
   * 异步关闭
   */
  CompletableFuture<REPLY> closeAsync();

  /**
   * 重写{@link AutoCloseable#close()}将异步关闭变成同步关闭
   * <p>
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
