package net.xdob.ratly.protocol;

import java.io.IOException;

public interface RaftClientProtocol {
  RaftClientReply submitClientRequest(RaftClientRequest request) throws IOException;
}