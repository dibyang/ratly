
package net.xdob.ratly.client.api;

import java.io.IOException;
import net.xdob.ratly.proto.raft.ReplicationLevel;
import net.xdob.ratly.protocol.exceptions.StaleReadException;
import net.xdob.ratly.protocol.Message;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftPeerId;

/**
 * Blocking API to support operations
 * such as sending message, read-message, stale-read-message and watch-request.
 * <p>
 * Note that this API supports a subset of the operations in {@link AsyncApi}.
 */
public interface BlockingApi {
  /**
   * Send the given message to the raft service.
   * The message may change the state of the service.
   * For readonly messages, use {@link #sendReadOnly(Message)} instead.
   *
   * @param message The request message.
   * @return the reply.
   */
  RaftClientReply send(Message message) throws IOException;

  /** The same as sendReadOnly(message, null). */
  default RaftClientReply sendReadOnly(Message message) throws IOException {
    return sendReadOnly(message, null);
  }

  /**
   * Send the given readonly message to the raft service.
   *
   * @param message The request message.
   * @param server The target server.  When server == null, send the message to the leader.
   * @return the reply.
   */
  RaftClientReply sendReadOnly(Message message, RaftPeerId server) throws IOException;

  /**
   * Send the given readonly message to the raft service using non-linearizable read.
   * This method is useful when linearizable read is enabled
   * but this client prefers not using it for performance reason.
   * When linearizable read is disabled, this method is the same as {@link #sendReadOnly(Message)}.
   *
   * @param message The request message.
   * @return the reply.
   */
  RaftClientReply sendReadOnlyNonLinearizable(Message message) throws IOException;

  /**
   * Send the given readonly message to the raft service.
   * The result will be read-after-write consistent, i.e. reflecting the latest successful write by the same client.
   * @param message The request message.
   * @return the reply.
   */
  RaftClientReply sendReadAfterWrite(Message message) throws IOException;

  /**
   * Send the given stale-read message to the given server (not the raft service).
   * If the server commit index is larger than or equal to the given min-index, the request will be processed.
   * Otherwise, the server throws a {@link StaleReadException}.
   *
   * @param message The request message.
   * @param minIndex The minimum log index that the server log must have already committed.
   * @param server The target server
   * @return the reply.
   */
  RaftClientReply sendStaleRead(Message message, long minIndex, RaftPeerId server) throws IOException;

  /**
   * Watch the given index to satisfy the given replication level.
   *
   * @param index The log index to be watched.
   * @param replication The replication level required.
   * @return the reply.
   *         When {@link RaftClientReply#isSuccess()} == true,
   *         the reply index (i.e. {@link RaftClientReply#getLogIndex()}) is the log index satisfying the request,
   *         where reply index >= watch index.
   */
  RaftClientReply watch(long index, ReplicationLevel replication) throws IOException;
}