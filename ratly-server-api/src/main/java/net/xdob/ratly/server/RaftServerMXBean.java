
package net.xdob.ratly.server;

import java.util.List;

/**
 * JMX information about the state of the current raft cluster.
 */
public interface RaftServerMXBean {

  /**
   * Identifier of the current server.
   */
  String getId();

  /**
   * Identifier of the leader node.
   */
  String getLeaderId();

  /**
   * Latest RAFT term.
   */
  long getCurrentTerm();

  /**
   * Cluster identifier.
   */
  String getGroupId();

  /**
   * RAFT Role of the server.
   */
  String getRole();

  /**
   * Addresses of the followers, only for leaders
   */
  List<String> getFollowers();

  /**
   * Gets the Groups of the Server.
   */
  List<String> getGroups();

}
