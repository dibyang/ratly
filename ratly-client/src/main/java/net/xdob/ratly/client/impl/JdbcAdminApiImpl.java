package net.xdob.ratly.client.impl;

import net.xdob.ratly.client.api.JdbcAdminApi;
import net.xdob.ratly.proto.jdbc.*;
import net.xdob.ratly.proto.sm.WrapReplyProto;
import net.xdob.ratly.proto.sm.WrapRequestProto;
import net.xdob.ratly.protocol.*;
import net.xdob.ratly.security.RsaHelper;
import net.xdob.ratly.util.Proto2Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class JdbcAdminApiImpl implements JdbcAdminApi {
	private final RaftClientImpl client;
	private final RsaHelper rsaHelper = new RsaHelper();

	JdbcAdminApiImpl(RaftClientImpl client) {
		this.client = Objects.requireNonNull(client, "client == null");
	}
	@Override
	public List<Map<String, Object>> showSession() throws SQLException {
		try {
			JdbcRequestProto requestProto = JdbcRequestProto.newBuilder()
					.setAdmin(AdminProto.newBuilder()
							.setCmd("show session")
							.build())
					.build();
			WrapRequestProto wrap = WrapRequestProto.newBuilder()
					.setType("db")
					.setJdbcRequest(requestProto)
					.build();
			RaftClientReply reply = client.io().sendAdmin(Message.valueOf(wrap));
			WrapReplyProto replyProto = WrapReplyProto.parseFrom(reply.getMessage().getContent());
			JdbcResponseProto response = replyProto.getJdbcResponse();
			if(response.hasEx()){
				throw Proto2Util.toThrowable(response.getEx(), SQLException.class);
			}
			return (List<Map<String,Object>>)Value.toJavaObject(response.getValue());
		} catch (IOException e) {
			throw new SQLException("io error:"+e.getMessage(), e);
		}
	}

	@Override
	public List<String> showDatabases() throws SQLException {
		try {
			JdbcRequestProto requestProto = JdbcRequestProto.newBuilder()
					.setAdmin(AdminProto.newBuilder()
							.setCmd("show databases")
							.build())
					.build();
			WrapRequestProto wrap = WrapRequestProto.newBuilder()
					.setType("db")
					.setJdbcRequest(requestProto)
					.build();
			RaftClientReply reply = client.io().sendAdmin(Message.valueOf(wrap));
			WrapReplyProto replyProto = WrapReplyProto.parseFrom(reply.getMessage().getContent());
			JdbcResponseProto response = replyProto.getJdbcResponse();
			if(response.hasEx()){
				throw Proto2Util.toThrowable(response.getEx(), SQLException.class);
			}
			return (List<String>)Value.toJavaObject(response.getValue());
		} catch (IOException e) {
			throw new SQLException("io error:"+e.getMessage(), e);
		}
	}

	@Override
	public boolean killSession(String sessionId) throws SQLException {
		try {
			JdbcRequestProto requestProto = JdbcRequestProto.newBuilder()
					.setAdmin(AdminProto.newBuilder()
							.setCmd("kill session")
							.setArg0(sessionId)
							.build())
					.build();
			WrapRequestProto wrap = WrapRequestProto.newBuilder()
					.setType("db")
					.setJdbcRequest(requestProto)
					.build();
			RaftClientReply reply = client.io().sendAdmin(Message.valueOf(wrap));
			WrapReplyProto replyProto = WrapReplyProto.parseFrom(reply.getMessage().getContent());
			JdbcResponseProto response = replyProto.getJdbcResponse();
			if(response.hasEx()){
				throw Proto2Util.toThrowable(response.getEx(), SQLException.class);
			}
			return response.hasUpdateCount() && response.getUpdateCount() > 0;
		} catch (IOException e) {
			throw new SQLException("io error:"+e.getMessage(), e);
		}
	}

	@Override
	public boolean createDatabase(String db, String user, String password) throws SQLException {
		try {
			JdbcRequestProto requestProto = JdbcRequestProto.newBuilder()
					.setAdmin(AdminProto.newBuilder()
							.setCmd("create db")
							.setArg0(db)
							.build())
					.setOpenSession(OpenSessionProto.newBuilder()
							.setUser( user)
							.setPassword( rsaHelper.encrypt(password)))
					.build();
			WrapRequestProto wrap = WrapRequestProto.newBuilder()
					.setType("db")
					.setJdbcRequest(requestProto)
					.build();
			RaftClientReply reply = client.io().send(Message.valueOf(wrap));
			WrapReplyProto replyProto = WrapReplyProto.parseFrom(reply.getMessage().getContent());
			JdbcResponseProto response = replyProto.getJdbcResponse();
			if(response.hasEx()){
				throw Proto2Util.toThrowable(response.getEx(), SQLException.class);
			}
			return response.hasUpdateCount() && response.getUpdateCount() > 0;
		} catch (IOException e) {
			throw new SQLException("io error:"+e.getMessage(), e);
		}
	}

	@Override
	public boolean dropDatabase(String db) throws SQLException {
		try {
			JdbcRequestProto requestProto = JdbcRequestProto.newBuilder()
					.setAdmin(AdminProto.newBuilder()
							.setCmd("drop db")
							.setArg0(db)
							.build())
					.build();
			WrapRequestProto wrap = WrapRequestProto.newBuilder()
					.setType("db")
					.setJdbcRequest(requestProto)
					.build();
			RaftClientReply reply = client.io().send(Message.valueOf(wrap));
			WrapReplyProto replyProto = WrapReplyProto.parseFrom(reply.getMessage().getContent());
			JdbcResponseProto response = replyProto.getJdbcResponse();
			if(response.hasEx()){
				throw Proto2Util.toThrowable(response.getEx(), SQLException.class);
			}
			return response.hasUpdateCount() && response.getUpdateCount() > 0;
		} catch (IOException e) {
			throw new SQLException("io error:"+e.getMessage(), e);
		}
	}
}
