package net.xdob.ratly.protocol;

import net.xdob.ratly.proto.raft.RaftPeerProto;
import net.xdob.ratly.proto.raft.RaftPeerRole;
import com.google.protobuf.ByteString;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.NetUtils;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A {@link RaftPeer} contains the information of a server.
 * <p>
 * The objects of this class are immutable.
 */
public final class RaftPeer {
	private static final RaftPeer[] EMPTY_ARRAY = {};

	/**
	 * @return an empty array.
	 */
	public static RaftPeer[] emptyArray() {
		return EMPTY_ARRAY;
	}

	/**
	 * 与 Raft 节点相关的添加操作接口。
	 */
	public interface Add {
		/**
		 * Add the given peers.
		 */
		void addRaftPeers(Collection<RaftPeer> peers);

		/**
		 * Add the given peers.
		 */
		default void addRaftPeers(RaftPeer... peers) {
			addRaftPeers(Arrays.asList(peers));
		}
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	/**
	 * @return a new builder initialized from {@code peer}
	 */
	public static Builder newBuilder(RaftPeer peer) {
		Objects.requireNonNull(peer, "peer == null");
		return newBuilder()
			.setId(peer.getId())
			.setAddress(peer.getAddress())
			.setAdminAddress(peer.getAdminAddress())
			.setClientAddress(peer.getClientAddress())
			.setDataStreamAddress(peer.getDataStreamAddress())
			.setPriority(peer.getPriority())
			.setStartupRole(peer.getStartupRole());
	}

	public static class Builder {
		private RaftPeerId id;
		private String address;
		private String adminAddress;
		private String clientAddress;
		private String dataStreamAddress;
		private int priority;
		private RaftPeerRole startupRole = RaftPeerRole.FOLLOWER;

		public Builder setId(RaftPeerId id) {
			this.id = id;
			return this;
		}

		public Builder setId(String id) {
			return setId(id, false);
		}

		public Builder setId(String id, boolean virtual) {
			return setId(RaftPeerId.valueOf(id, virtual));
		}

		public Builder setId(ByteString id) {
			return setId(RaftPeerId.valueOf(id));
		}

		public Builder setAddress(String address) {
			this.address = address;
			return this;
		}

		public Builder setAddress(InetSocketAddress address) {
			return setAddress(NetUtils.address2String(address));
		}

		public Builder setAdminAddress(String addr) {
			this.adminAddress = addr;
			return this;
		}

		public Builder setAdminAddress(InetSocketAddress addr) {
			return setAdminAddress(NetUtils.address2String(addr));
		}

		public Builder setClientAddress(String addr) {
			this.clientAddress = addr;
			return this;
		}

		public Builder setClientAddress(InetSocketAddress addr) {
			return setClientAddress(NetUtils.address2String(addr));
		}

		public Builder setDataStreamAddress(String dataStreamAddress) {
			this.dataStreamAddress = dataStreamAddress;
			return this;
		}

		public Builder setDataStreamAddress(InetSocketAddress dataStreamAddress) {
			return setDataStreamAddress(NetUtils.address2String(dataStreamAddress));
		}

		public Builder setPriority(int priority) {
			if (priority < 0) {
				throw new IllegalArgumentException("priority = " + priority + " < 0");
			}
			this.priority = priority;
			return this;
		}


		public Builder setStartupRole(RaftPeerRole startupRole) {
			if (startupRole != RaftPeerRole.FOLLOWER
				&& startupRole != RaftPeerRole.LISTENER) {
				throw new IllegalArgumentException(
					"At startup the role can only be set to FOLLOWER or LISTENER, the current value is " +
						startupRole);
			}
			this.startupRole = startupRole;
			return this;
		}

		public RaftPeer build() {
			return new RaftPeer(
				Objects.requireNonNull(id, "The 'id' field is not initialized."),
				address, adminAddress, clientAddress, dataStreamAddress, priority, startupRole);
		}
	}

	/**
	 * The id of the peer.
	 */
	private final RaftPeerId id;
	/**
	 * The RPC address of the peer.
	 */
	private final String address;
	private final String adminAddress;
	private final String clientAddress;
	/**
	 * The DataStream address of the peer.
	 */
	private final String dataStreamAddress;
	/**
	 * The priority of the peer.
	 */
	private final int priority;



	private final RaftPeerRole startupRole;

	private final Supplier<RaftPeerProto> raftPeerProto;

	private RaftPeer(RaftPeerId id,
									 String address, String adminAddress, String clientAddress, String dataStreamAddress,
									 int priority, RaftPeerRole startupRole) {
		this.id = Objects.requireNonNull(id, "id == null");
		this.address = address;
		this.dataStreamAddress = dataStreamAddress;
		this.adminAddress = adminAddress;
		this.clientAddress = clientAddress;
		this.priority = priority;
		this.startupRole = startupRole;
		this.raftPeerProto = JavaUtils.memoize(this::buildRaftPeerProto);
	}

	private RaftPeerProto buildRaftPeerProto() {
		final RaftPeerProto.Builder builder = RaftPeerProto.newBuilder()
			.setId(getId().toByteString());
		Optional.ofNullable(getAddress()).ifPresent(builder::setAddress);
		Optional.ofNullable(getDataStreamAddress()).ifPresent(builder::setDataStreamAddress);
		Optional.ofNullable(getClientAddress()).ifPresent(builder::setClientAddress);
		Optional.ofNullable(getAdminAddress()).ifPresent(builder::setAdminAddress);
		builder.setPriority(priority);
		builder.setStartupRole(startupRole);
		return builder.build();
	}

	/**
	 * @return The id of the peer.
	 */
	public RaftPeerId getId() {
		return id;
	}

	/**
	 * 是否虚拟节点
	 */
	public boolean isVirtual() {
		return id.isVirtual();
	}

	/**
	 * @return The RPC address of the peer for server-server communication.
	 */
	public String getAddress() {
		return address;
	}

	/**
	 * @return The RPC address of the peer for admin operations.
	 * May be {@code null}, in which case {@link #getAddress()} should be used.
	 */
	public String getAdminAddress() {
		return adminAddress;
	}

	/**
	 * @return The RPC address of the peer for client operations.
	 * May be {@code null}, in which case {@link #getAddress()} should be used.
	 */
	public String getClientAddress() {
		return clientAddress;
	}

	/**
	 * @return The data stream address of the peer.
	 */
	public String getDataStreamAddress() {
		return dataStreamAddress;
	}

	/**
	 * @return The priority of the peer.
	 */
	public int getPriority() {
		return priority;
	}

	public RaftPeerRole getStartupRole() {
		return startupRole;
	}

	public RaftPeerProto getRaftPeerProto() {
		return raftPeerProto.get();
	}

	@Override
	public String toString() {
		final String rpc = address != null ? "|" + address : "";
		return id + rpc;
	}

	public String getDetails() {
		final String prefix = toString();
		final String admin = adminAddress != null && !Objects.equals(address, adminAddress)
			? "|admin:" + adminAddress : "";
		final String client = clientAddress != null && !Objects.equals(address, clientAddress)
			? "|client:" + clientAddress : "";
		final String data = dataStreamAddress != null ? "|dataStream:" + dataStreamAddress : "";
		final String p = "|priority:" + priority;
		final String role = "|startupRole:" + startupRole;
		return prefix + admin + client + data + p + role;
	}

	@Override
	public boolean equals(Object o) {
		return (o instanceof RaftPeer) && id.equals(((RaftPeer) o).getId());
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}
}
