
package net.xdob.ratly.client.api;

import net.xdob.ratly.protocol.GroupInfoReply;
import net.xdob.ratly.protocol.GroupListReply;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftGroup;
import net.xdob.ratly.protocol.RaftGroupId;

import java.io.IOException;

/**
 * APIs to support group management operations such as add, remove, list and info to a particular server.
 */
public interface GroupManagementApi {
  /**
   * Add a new group.
   * @param format Should it format the storage?
   */
  RaftClientReply add(RaftGroup newGroup, boolean format) throws IOException;

  /** The same as add(newGroup, true). */
  default RaftClientReply add(RaftGroup newGroup) throws IOException {
    return add(newGroup, true);
  }

  /** Remove a group. */
  RaftClientReply remove(RaftGroupId groupId, boolean deleteDirectory, boolean renameDirectory) throws IOException;

  /** List all the groups.*/
  GroupListReply list() throws IOException;

  /** Get the info of the given group.*/
  GroupInfoReply info(RaftGroupId group) throws IOException;
}