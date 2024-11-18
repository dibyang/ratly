package net.xdob.ratly.protocol;

public interface RaftRpcMessage {

  boolean isRequest();

  String getRequestorId();

  String getReplierId();

  RaftGroupId getRaftGroupId();
}
