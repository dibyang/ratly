
package net.xdob.ratly.server;

import java.util.List;

/**
 *  Raft 集群状态的 JMX 信息。这些信息通常用于监控和管理 Raft 集群的运行状况，
 *  通过 JMX (Java Management Extensions) 提供集群的各种运行时数据。
 */
public interface RaftServerMXBean {

  /**
   * 描述：返回当前服务器的唯一标识符（ID）。
   * 用途：用于标识当前服务器，在集群中有多个服务器，每个服务器都具有唯一的 Id。
   */
  String getId();

  /**
   * 描述：返回当前 Raft 集群的领导者节点（Leader）的标识符。
   * 用途：Raft 协议中的领导者负责处理所有客户端的请求，因此知道领导者的 ID 对于集群监控和管理非常重要。
   */
  String getLeaderId();

  /**
   * Latest RAFT term.
   * 描述：返回当前 Raft 协议的任期号（Term）。
   * 用途：Raft 协议中，集群的状态由任期（Term）控制。每当发生领导者选举时，任期号都会递增。这个方法可以提供当前集群所处的任期状态。
   */
  long getCurrentTerm();

  /**
   * Cluster identifier.
   * 描述：返回当前集群的标识符（Group ID）。
   * 用途：Raft 协议通常支持多个集群。这个方法用于获取当前集群的 ID，帮助区分不同的集群。
   */
  String getGroupId();

  /**
   * RAFT Role of the server.
   * 描述：返回当前服务器在 Raft 集群中的角色（如 Leader、Follower、Candidate）。
   * 用途：Raft 集群中的每个节点都有不同的角色，了解当前服务器的角色对于故障排查、集群管理等操作非常有用。
   */
  String getRole();

  /**
   * Addresses of the followers, only for leaders
   * 描述：仅在领导者节点中有效，返回所有跟随者（Follower）的地址。
   * 用途：领导者节点通常需要知道并管理所有跟随者节点的状态，获取跟随者列表对于监控集群的健康状态和进行领导者选举非常重要。
   */
  List<String> getFollowers();

  /**
   * 描述：返回当前服务器所属的所有 Raft 集群组的列表。
   * 用途：一个服务器可以加入多个集群，使用此方法可以获取当前服务器参与的所有集群的 ID 或其他标识信息。
   */
  List<String> getGroups();

}
