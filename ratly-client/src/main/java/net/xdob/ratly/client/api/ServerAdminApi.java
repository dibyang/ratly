
package net.xdob.ratly.client.api;

import net.xdob.ratly.protocol.RaftClientReply;

import java.io.IOException;

/**
 * An API to support control server state
 */
public interface ServerAdminApi {

  RaftClientReply stopServer(long timeoutMs) throws IOException;
	RaftClientReply startServer(long timeoutMs) throws IOException;
	RaftClientReply setAutoStartServer(boolean autoStart, long timeoutMs) throws IOException;
	RaftClientReply getAutoStartServer(long timeoutMs) throws IOException;
}
