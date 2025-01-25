package net.xdob.ratly.rmap;

import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.protocol.SerialSupport;

import java.io.IOException;

public interface DContext {
  RaftClient getClient();
  SerialSupport getFasts();
  PutReply sendPutRequest(PutRequest putRequest) throws IOException;
  GetReply sendGetRequest(GetRequest getRequest) throws IOException;
}
