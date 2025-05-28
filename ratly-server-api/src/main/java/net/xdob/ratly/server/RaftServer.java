package net.xdob.ratly.server;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.Lists;
import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.protocol.AdminAsynchronousProtocol;
import net.xdob.ratly.protocol.AdminProtocol;
import net.xdob.ratly.protocol.RaftClientAsynchronousProtocol;
import net.xdob.ratly.protocol.RaftClientProtocol;
import net.xdob.ratly.protocol.RaftGroup;
import net.xdob.ratly.protocol.RaftGroupId;
import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.rpc.RpcType;
import net.xdob.ratly.server.protocol.RaftServerAsynchronousProtocol;
import net.xdob.ratly.server.protocol.RaftServerProtocol;
import net.xdob.ratly.server.storage.StartupOption;
import net.xdob.ratly.statemachine.StateMachine;
import net.xdob.ratly.util.IOUtils;
import net.xdob.ratly.util.LifeCycle;
import net.xdob.ratly.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raft 协议的服务端实现，负责处理集群中的节点间的通信、领导者选举、日志复制等操作。
 */
public interface RaftServer extends Closeable, RpcType.Get,
    RaftServerProtocol, RaftServerAsynchronousProtocol,
    RaftClientProtocol, RaftClientAsynchronousProtocol,
    AdminProtocol, AdminAsynchronousProtocol,
    BeansFactory{
  Logger LOG = LoggerFactory.getLogger(RaftServer.class);

  /**
   * 返回 RaftServer 的唯一 ID (RaftPeerId)。
   * @return the server ID.
   */
  RaftPeerId getId();

  /**
   * 返回与该服务器相关的 RaftPeer，可以用于获取当前节点的详细信息。
   * @return the general {@link RaftPeer} for this server.
   *         To obtain a specific {@link RaftPeer} for a {@link RaftGroup}, use {@link Division#getPeer()}.
   */
  RaftPeer getPeer();

  /**
   * 返回该服务器所在的所有 Raft 集群组 ID。
   * @return the group IDs the server is part of.
   */
  Iterable<RaftGroupId> getGroupIds();

  /**
   * 返回该服务器参与的所有 Raft 集群组。
   * @return the groups the server is part of.
   */
  Iterable<RaftGroup> getGroups() throws IOException;

  /**
   * 根据集群组 ID 获取该服务器对应的 Division 实例。
   */
  Division getDivision(RaftGroupId groupId) throws IOException;

  /**
   * 返回服务器的配置属性 (RaftProperties)。
   * @return the server properties.
   */
  RaftProperties getProperties();

  /**
   * 返回用于处理 RPC 请求的服务接口 ({@link RaftServerRpc})。
   * @return the rpc service.
   */
  RaftServerRpc getServerRpc();

  /**
   * 返回用于数据流传输的 RPC 服务接口 ({@link DataStreamServerRpc})。
   * @return the data stream rpc service.
   */
  DataStreamServerRpc getDataStreamServerRpc();

  /**
   * 返回该服务器的 RPC 类型 (RpcType)，用于标识服务器支持的协议类型。
   * @return the {@link RpcType}.
   */
  default RpcType getRpcType() {
    return getFactory().getRpcType();
  }

  /**
   * 返回用于创建 RaftServer 组件的工厂实例 (ServerFactory)。
   * @return the factory for creating server components.
   */
  ServerFactory getFactory();

  /**
   * 启动该 Raft 服务器。
   * Start this server.
   */
  void start() throws IOException;

  /**
   * 返回服务器的生命周期状态。
   */
  LifeCycle.State getLifeCycleState();

  /** @return a {@link Builder}. */
  static Builder newBuilder() {
    return new Builder();
  }

  /** To build {@link RaftServer} objects. */
  class Builder {
    private static final Method NEW_RAFT_SERVER_METHOD = initNewRaftServerMethod();

    private static Method initNewRaftServerMethod() {
      final String className = RaftServer.class.getPackage().getName() + ".impl.ServerImplUtils";
      final Class<?>[] argClasses = {RaftPeerId.class, RaftGroup.class, StartupOption.class,
          StateMachine.Registry.class, ThreadGroup.class, RaftProperties.class, Parameters.class};
      try {
        final Class<?> clazz = ReflectionUtils.getClassByName(className);
        return clazz.getMethod("newRaftServer", argClasses);
      } catch (Exception e) {
        throw new IllegalStateException("Failed to initNewRaftServerMethod", e);
      }
    }

    private static RaftServer newRaftServer(RaftPeerId serverId, RaftGroup group, StartupOption option,
        StateMachine.Registry stateMachineRegistry, ThreadGroup threadGroup, RaftProperties properties,
        Parameters parameters) throws IOException {
      try {
        return (RaftServer) NEW_RAFT_SERVER_METHOD.invoke(null,
            serverId, group, option, stateMachineRegistry, threadGroup, properties, parameters);
      } catch (IllegalAccessException e) {
        throw new IllegalStateException("Failed to build " + serverId, e);
      } catch (InvocationTargetException e) {
        throw IOUtils.asIOException(e.getCause());
      }
    }

    private RaftPeerId serverId;
    private StateMachine.Registry stateMachineRegistry ;
    private RaftGroup group = null;
    private StartupOption option = StartupOption.FORMAT;
    private RaftProperties properties;
    private Parameters parameters;
    private ThreadGroup threadGroup;
    private final List<BeanFinder> beanFinders = Lists.newCopyOnWriteArrayList();


    /** @return a {@link RaftServer} object. */
    public RaftServer build() throws IOException {
      RaftServer raftServer = newRaftServer(
          serverId,
          group,
          option,
          Objects.requireNonNull(stateMachineRegistry, "Neither 'stateMachine' nor 'setStateMachineRegistry' " +
              "is initialized."),
          threadGroup,
          Objects.requireNonNull(properties, "The 'properties' field is not initialized."),
          parameters);
      beanFinders.forEach(raftServer::addBeanFinder);
      return raftServer;
    }

    public Builder addBeanFinder(BeanFinder beanFinder){
      beanFinders.add(beanFinder);
      return this;
    }

    public Builder removeBeanFinder(BeanFinder beanFinder){
      beanFinders.remove(beanFinder);
      return this;
    }

    /** Set the server ID. */
    public Builder setServerId(RaftPeerId serverId) {
      this.serverId = serverId;
      return this;
    }

    /** Set the {@link StateMachine} of the server. */
    public Builder setStateMachine(StateMachine stateMachine) {
      return setStateMachineRegistry(gid -> stateMachine);
    }

    /** Set the {@link StateMachine.Registry} of the server. */
    public Builder setStateMachineRegistry(StateMachine.Registry stateMachineRegistry ) {
      this.stateMachineRegistry = stateMachineRegistry ;
      return this;
    }

    /** Set all the peers (including the server being built) in the Raft cluster. */
    public Builder setGroup(RaftGroup group) {
      this.group = group;
      return this;
    }

    /** Set the startup option for the group. */
    public Builder setOption(StartupOption option) {
      this.option = option;
      return this;
    }

    /** Set {@link RaftProperties}. */
    public Builder setProperties(RaftProperties properties) {
      this.properties = properties;
      return this;
    }

    /** Set {@link Parameters}. */
    public Builder setParameters(Parameters parameters) {
      this.parameters = parameters;
      return this;
    }

    /**
     * Set {@link ThreadGroup} so the application can control RaftServer threads consistently with the application.
     * For example, configure {@link ThreadGroup#uncaughtException(Thread, Throwable)} for the whole thread group.
     * If not set, the new thread will be put into the thread group of the caller thread.
     */
    public Builder setThreadGroup(ThreadGroup threadGroup) {
      this.threadGroup = threadGroup;
      return this;
    }
  }
}
