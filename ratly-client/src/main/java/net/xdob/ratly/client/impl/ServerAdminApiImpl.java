package net.xdob.ratly.client.impl;

import net.xdob.ratly.client.api.ServerAdminApi;
import net.xdob.ratly.protocol.RaftClientReply;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.protocol.ServerAdminRequest;
import net.xdob.ratly.protocol.SnapshotManagementRequest;
import net.xdob.ratly.rpc.CallId;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class ServerAdminApiImpl implements ServerAdminApi {
	private final RaftClientImpl client;
	private final RaftPeerId server;

	ServerAdminApiImpl(RaftPeerId server, RaftClientImpl client) {
		this.server = server;
		this.client = Objects.requireNonNull(client, "client == null");
	}


	@Override
	public RaftClientReply stopServer(long timeoutMs) throws IOException {
		final long callId = CallId.getAndIncrement();
		return client.io().sendRequestWithRetry(() -> ServerAdminRequest.newStop(client.getId(),
				Optional.ofNullable(server).orElseGet(client::getLeaderId),
				client.getGroupId(), callId, timeoutMs));
	}

	@Override
	public RaftClientReply startServer(long timeoutMs) throws IOException {
		final long callId = CallId.getAndIncrement();
		return client.io().sendRequestWithRetry(() -> ServerAdminRequest.newStart(client.getId(),
				Optional.ofNullable(server).orElseGet(client::getLeaderId),
				client.getGroupId(), callId, timeoutMs));
	}

	@Override
	public RaftClientReply setAutoStartServer(boolean autoStart, long timeoutMs) throws IOException {
		final long callId = CallId.getAndIncrement();
		return client.io().sendRequestWithRetry(() -> ServerAdminRequest.newSetAutoSTart(client.getId(),
				Optional.ofNullable(server).orElseGet(client::getLeaderId),
				client.getGroupId(), callId, autoStart, timeoutMs));
	}

	@Override
	public RaftClientReply getAutoStartServer(long timeoutMs) throws IOException {
		final long callId = CallId.getAndIncrement();
		return client.io().sendRequestWithRetry(() -> ServerAdminRequest.newGetAutoSTart(client.getId(),
				Optional.ofNullable(server).orElseGet(client::getLeaderId),
				client.getGroupId(), callId, timeoutMs));
	}
}
