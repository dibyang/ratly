package net.xdob.ratly.protocol;

import net.xdob.ratly.util.function.SerialFunction;

public class DRpcRequest<T, R> extends RaftClientRequest {
  private final BeanTarget<T> target;
  private final SerialFunction<T,R> fun;
  public DRpcRequest(ClientId clientId, RaftPeerId serverId, RaftGroupId groupId, long callId, BeanTarget<T> target, SerialFunction<T, R> fun) {
    super(clientId, serverId, groupId, callId, readRequestType());
    this.target = target;
    this.fun = fun;
  }

  public BeanTarget<T> getTarget() {
    return target;
  }

  public SerialFunction<T,R> getFun() {
    return fun;
  }
}
