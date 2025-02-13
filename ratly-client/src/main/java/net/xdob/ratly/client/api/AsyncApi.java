
package net.xdob.ratly.client.api;

import java.util.concurrent.CompletableFuture;
import net.xdob.ratly.proto.raft.ReplicationLevel;
import net.xdob.ratly.protocol.exceptions.StaleReadException;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftPeerId;

/**
 * Asynchronous API to support operations
 * such as sending message, read-message, stale-read-message and watch-request.
 * <p>
 * Note that this API supports all the operations in {@link BlockingApi}.
 */
public interface AsyncApi {
  /**
   * Send the given message asynchronously to the raft service.
   * The message may change the state of the service.
   * For readonly messages, use {@link #sendReadOnly(Message)} instead.
   *
   * @param message The request message.
   * @param replication The replication level to wait for.
   * @return a future of the reply.
   */
  CompletableFuture<RaftClientReply> send(Message message, ReplicationLevel replication);

  /** The same as send(message, ReplicationLevel.MAJORITY). */
  default CompletableFuture<RaftClientReply> send(Message message) {
    return send(message, ReplicationLevel.MAJORITY);
  }

  /** The same as sendReadOnly(message, null). */
  default CompletableFuture<RaftClientReply> sendReadOnly(Message message) {
    return sendReadOnly(message, null);
  }

  /**
   * Send the given readonly message asynchronously to the raft service.
   * Note that the reply futures are completed in the same order of the messages being sent.
   *
   * @param message The request message.
   * @param server The target server.  When server == null, send the message to the leader.
   * @return a future of the reply.
   */
  CompletableFuture<RaftClientReply> sendReadOnly(Message message, RaftPeerId server);

  /**
   * Send the given readonly message asynchronously to the raft service.
   * The result will be read-after-write consistent, i.e. reflecting the latest successful write by the same client.
   * @param message The request message.
   * @return the reply.
   */
  CompletableFuture<RaftClientReply> sendReadAfterWrite(Message message);


  /**
   * Send the given readonly message asynchronously to the raft service using non-linearizable read.
   * This method is useful when linearizable read is enabled
   * but this client prefers not using it for performance reason.
   * When linearizable read is disabled, this method is the same as {@link #sendReadOnly(Message)}.
   *
   * @param message The request message.
   * @return a future of the reply.
   */
  CompletableFuture<RaftClientReply> sendReadOnlyNonLinearizable(Message message);

  /** The same as sendReadOnlyUnordered(message, null). */
  default CompletableFuture<RaftClientReply> sendReadOnlyUnordered(Message message) {
    return sendReadOnlyUnordered(message, null);
  }

  /**
   * Send the given readonly message asynchronously to the raft service.
   * Note that the reply futures can be completed in any order.
   *
   * @param message The request message.
   * @param server The target server.  When server == null, send the message to the leader.
   * @return a future of the reply.
   */
  CompletableFuture<RaftClientReply> sendReadOnlyUnordered(Message message, RaftPeerId server);

  /**
   * Send the given stale-read message asynchronously to the given server (not the raft service).
   * If the server commit index is larger than or equal to the given min-index, the request will be processed.
   * Otherwise, the server returns a {@link StaleReadException}.
   *
   * @param message The request message.
   * @param minIndex The minimum log index that the server log must have already committed.
   * @param server The target server
   * @return a future of the reply.
   */
  CompletableFuture<RaftClientReply> sendStaleRead(Message message, long minIndex, RaftPeerId server);

  /**
   * Watch the given index asynchronously to satisfy the given replication level.
   *
   * @param index The log index to be watched.
   * @param replication The replication level required.
   * @return a future of the reply.
   *         When {@link RaftClientReply#isSuccess()} == true,
   *         the reply index (i.e. {@link RaftClientReply#getLogIndex()}) is the log index satisfying the request,
   *         where reply index >= watch index.
   */
  CompletableFuture<RaftClientReply> watch(long index, ReplicationLevel replication);
}
