
package net.xdob.ratly.client.api;

import net.xdob.ratly.protocol.RaftClientReply;

import java.io.IOException;

/**
 * An API to support control snapshot
 * such as create and list snapshot file.
 */
public interface SnapshotManagementApi {

  /** The same as create(0, timeoutMs). */
  default RaftClientReply create(long timeoutMs) throws IOException {
    return create(0, timeoutMs);
  }

    /** The same as create(force? 1 : 0, timeoutMs). */
  default RaftClientReply create(boolean force, long timeoutMs) throws IOException {
    return create(force? 1 : 0, timeoutMs);
  }

    /**
   * Trigger to create a snapshot.
   *
   * @param creationGap When (creationGap > 0) and (astAppliedIndex - lastSnapshotIndex < creationGap),
   *                    return lastSnapshotIndex; otherwise, take a new snapshot and then return its index.
   *                    When creationGap == 0, use the server configured value as the creationGap.
   * @return a reply.  When {@link RaftClientReply#isSuccess()} is true,
   *         {@link RaftClientReply#getLogIndex()} is the snapshot index fulfilling the operation.
   */
  RaftClientReply create(long creationGap, long timeoutMs) throws IOException;
}
