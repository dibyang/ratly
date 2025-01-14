package net.xdob.ratly.protocol;

import com.google.common.collect.Lists;

public class DRpcReply<R> extends RaftClientReply{
  private final R data;
  private final Exception ex;
  public DRpcReply(ClientId clientId, RaftPeerId serverId, RaftGroupId groupId, long callId, R data, Exception ex) {
    super(clientId, serverId, groupId, callId, true, null, null, 0L, Lists.newArrayList());
    this.data = data;
    this.ex = ex;
  }

  public DRpcReply(RaftClientRequest request, R data, Exception ex) {
    this(request.getClientId(), request.getServerId(), request.getRaftGroupId(), request.getCallId(),
        data, ex);
  }

  public R getData() {
    return data;
  }

  public Exception getEx() {
    return ex;
  }
}
