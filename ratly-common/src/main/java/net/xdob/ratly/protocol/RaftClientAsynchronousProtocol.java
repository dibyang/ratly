package net.xdob.ratly.protocol;

import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.ReferenceCountedObject;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** Asynchronous version of {@link RaftClientProtocol}. */
public interface RaftClientAsynchronousProtocol {
  /**
   * It is recommended to override {@link #submitClientRequestAsync(ReferenceCountedObject)} instead.
   * Then, it does not have to override this method.
   */
  default CompletableFuture<RaftClientReply> submitClientRequestAsync(
      RaftClientRequest request) throws IOException {
    return submitClientRequestAsync(ReferenceCountedObject.wrap(request));
  }

  /**
   * A referenced counted request is submitted from a client for processing.
   * Implementations of this method should retain the request, process it and then release it.
   * The request may be retained even after the future returned by this method has completed.
   *
   * @return a future of the reply
   * @see ReferenceCountedObject
   */
  default CompletableFuture<RaftClientReply> submitClientRequestAsync(
      ReferenceCountedObject<RaftClientRequest> requestRef) {
    try {
      // for backward compatibility
      return submitClientRequestAsync(requestRef.retain());
    } catch (Exception e) {
      return JavaUtils.completeExceptionally(e);
    } finally {
      requestRef.release();
    }
  }
}