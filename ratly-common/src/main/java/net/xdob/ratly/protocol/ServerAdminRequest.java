package net.xdob.ratly.protocol;

import net.xdob.ratly.util.JavaUtils;

public final class ServerAdminRequest extends RaftClientRequest {

  public abstract static class Op {

  }

  public static final class Start extends Op {
    private Start() {

    }

    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass()) + ":" ;
    }

  }

	public static final class Stop extends Op {
		private Stop() {

		}

		@Override
		public String toString() {
			return JavaUtils.getClassSimpleName(getClass()) + ":" ;
		}

	}

	public static final class GetAutoSTart extends Op {
		private GetAutoSTart() {

		}

		@Override
		public String toString() {
			return JavaUtils.getClassSimpleName(getClass()) + ":" ;
		}

	}

	public static final class SetAutoSTart extends Op {
		private final boolean autoStart;
		private SetAutoSTart(boolean autoStart) {
			this.autoStart = autoStart;
		}

		public boolean isAutoStart() {
			return autoStart;
		}

		@Override
		public String toString() {
			return JavaUtils.getClassSimpleName(getClass()) + ":" ;
		}

	}


  public static ServerAdminRequest newStart(ClientId clientId,
																						 RaftPeerId serverId, RaftGroupId groupId, long callId, long timeoutMs) {
    return new ServerAdminRequest(clientId,
        serverId, groupId, callId, timeoutMs, new Start());
  }

	public static ServerAdminRequest newStop(ClientId clientId,
																						RaftPeerId serverId, RaftGroupId groupId, long callId, long timeoutMs) {
		return new ServerAdminRequest(clientId,
				serverId, groupId, callId, timeoutMs, new Stop());
	}

	public static ServerAdminRequest newGetAutoSTart(ClientId clientId,
																					 RaftPeerId serverId, RaftGroupId groupId, long callId, long timeoutMs) {
		return new ServerAdminRequest(clientId,
				serverId, groupId, callId, timeoutMs, new GetAutoSTart());
	}

	public static ServerAdminRequest newSetAutoSTart(ClientId clientId,
																									 RaftPeerId serverId, RaftGroupId groupId, long callId, boolean autoStart,long timeoutMs) {
		return new ServerAdminRequest(clientId,
				serverId, groupId, callId, timeoutMs, new SetAutoSTart(autoStart));
	}

  private final Op op;

  public ServerAdminRequest(ClientId clientId,
														RaftPeerId serverId, RaftGroupId groupId, long callId, long timeoutMs, Op op) {
    super(clientId, serverId, groupId, callId, readRequestType(), timeoutMs);
    this.op = op;
  }

  public Start getStart() {
    return op instanceof Start ? (Start)op: null;
  }

	public Stop getStop() {
		return op instanceof Stop ? (Stop)op: null;
	}

	public GetAutoSTart getGetAutoSTart() {
		return op instanceof GetAutoSTart ? (GetAutoSTart)op: null;
	}

	public SetAutoSTart getSetAutoSTart() {
		return op instanceof SetAutoSTart ? (SetAutoSTart)op: null;
	}

  @Override
  public String toString() {
    return super.toString() + ", " + op;
  }
}
