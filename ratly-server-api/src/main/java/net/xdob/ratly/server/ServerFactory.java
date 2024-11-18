
package net.xdob.ratly.server;

import net.xdob.ratly.rpc.RpcFactory;
import net.xdob.ratly.server.leader.FollowerInfo;
import net.xdob.ratly.server.leader.LeaderState;
import net.xdob.ratly.server.leader.LogAppender;

/** A factory interface for creating server components. */
public interface ServerFactory extends RpcFactory {
  static ServerFactory cast(RpcFactory rpcFactory) {
    if (rpcFactory instanceof ServerFactory) {
      return (ServerFactory)rpcFactory;
    }
    throw new ClassCastException("Cannot cast " + rpcFactory.getClass()
        + " to " + ServerFactory.class
        + "; rpc type is " + rpcFactory.getRpcType());
  }

  /** Create a new {@link LogAppender}. */
  default LogAppender newLogAppender(RaftServer.Division server, LeaderState state, FollowerInfo f) {
    return LogAppender.newLogAppenderDefault(server, state, f);
  }

  RaftServerRpc newRaftServerRpc(RaftServer server);
}
