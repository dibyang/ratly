

package net.xdob.ratly.server.protocol;

import net.xdob.ratly.proto.RaftProtos.ReadIndexRequestProto;
import net.xdob.ratly.proto.RaftProtos.ReadIndexReplyProto;
import net.xdob.ratly.proto.RaftProtos.AppendEntriesReplyProto;
import net.xdob.ratly.proto.RaftProtos.AppendEntriesRequestProto;
import net.xdob.ratly.util.ReferenceCountedObject;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public interface RaftServerAsynchronousProtocol {

  /**
   * It is recommended to override {@link #appendEntriesAsync(ReferenceCountedObject)} instead.
   * Then, it does not have to override this method.
   */
  default CompletableFuture<AppendEntriesReplyProto> appendEntriesAsync(AppendEntriesRequestProto request)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * A referenced counted request is submitted from a client for processing.
   * Implementations of this method should retain the request, process it and then release it.
   * The request may be retained even after the future returned by this method has completed.
   *
   * @return a future of the reply
   * @see ReferenceCountedObject
   */
  default CompletableFuture<AppendEntriesReplyProto> appendEntriesAsync(
      ReferenceCountedObject<AppendEntriesRequestProto> requestRef) throws IOException {
    // Default implementation for backward compatibility.
    try {
      return appendEntriesAsync(requestRef.retain());
    } finally {
      requestRef.release();
    }
  }

  CompletableFuture<ReadIndexReplyProto> readIndexAsync(ReadIndexRequestProto request)
      throws IOException;
}
