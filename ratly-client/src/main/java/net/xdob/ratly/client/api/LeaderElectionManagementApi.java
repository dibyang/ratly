
package net.xdob.ratly.client.api;

import net.xdob.ratly.protocol.RaftClientReply;

import java.io.IOException;

/**
 * An API to support control leader election
 * such as pause and resume election
 */
public interface LeaderElectionManagementApi {

  /** pause leader election. */
  RaftClientReply pause() throws IOException;

  /** resume leader election. */
  RaftClientReply resume() throws IOException;

}
