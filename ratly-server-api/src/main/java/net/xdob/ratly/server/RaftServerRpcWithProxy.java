
package net.xdob.ratly.server;

import net.xdob.ratly.protocol.RaftPeer;
import net.xdob.ratly.protocol.RaftPeerId;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.LifeCycle;
import net.xdob.ratly.util.PeerProxyMap;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 抽象类 RaftServerRpcWithProxy实现了 {@link RaftServerRpc} 接口，
 * 并通过代理模式（{@link PeerProxyMap}）为不同的 Raft 节点提供服务。
 * 该类的设计使得服务器能够使用代理对象来管理与各个 Raft 节点的通信。
 * <p>
 * 泛型：
 *    1.PROXY：代表代理对象的类型，必须实现 Closeable 接口，说明这些代理对象需要在适当的时候被关闭。
 *    2.PROXIES：代表 PeerProxyMap 的类型，PeerProxyMap 用于管理和存储与各个 Raft 节点（Peer）相关的代理对象。
 */
public abstract class RaftServerRpcWithProxy<PROXY extends Closeable, PROXIES extends PeerProxyMap<PROXY>>
    implements RaftServerRpc {
  private final Supplier<RaftPeerId> idSupplier;
  private final Supplier<LifeCycle> lifeCycleSupplier;
  private final Supplier<PROXIES> proxiesSupplier;

  /**
   *
   * @param idSupplier 提供当前服务器 ID 的供应器（Supplier）。这使得服务器 ID 的获取变得灵活且延迟。
   * @param proxyCreater 一个函数，根据服务器 ID 创建 PeerProxyMap（代理对象集合）。这个函数为每个 Raft 节点创建一个代理对象。
   */
  protected RaftServerRpcWithProxy(Supplier<RaftPeerId> idSupplier, Function<RaftPeerId, PROXIES> proxyCreater) {
    this.idSupplier = idSupplier;
    this.lifeCycleSupplier = JavaUtils.memoize(
        () -> new LifeCycle(getId() + "-" + JavaUtils.getClassSimpleName(getClass())));
    this.proxiesSupplier = JavaUtils.memoize(() -> proxyCreater.apply(getId()));
  }

  /** @return the server id. */
  public RaftPeerId getId() {
    return idSupplier.get();
  }

  /**
   * 描述：返回一个代理对象集合 PeerProxyMap。
   * 实现：proxiesSupplier 通过延迟加载返回 PeerProxyMap，该集合管理着所有 Raft 节点的代理对象。
   */
  private LifeCycle getLifeCycle() {
    return lifeCycleSupplier.get();
  }

  /** @return the underlying {@link PeerProxyMap}. */
  public PROXIES getProxies() {
    return proxiesSupplier.get();
  }

  @Override
  public void addRaftPeers(Collection<RaftPeer> peers) {
    getProxies().addRaftPeers(peers);
  }

  @Override
  public void handleException(RaftPeerId serverId, Exception e, boolean reconnect) {
    getProxies().handleException(serverId, e, reconnect);
  }

  @Override
  public final void start() throws IOException {
    getLifeCycle().startAndTransition(this::startImpl, IOException.class);
  }

  /** Implementation of the {@link #start()} method. */
  protected abstract void startImpl() throws IOException;

  /**
   * 描述：关闭服务器。
   * 实现：调用生命周期的 checkStateAndClose() 方法，在关闭服务器之前检查当前的状态，并执行 closeImpl() 方法
   */
  @Override
  public final void close() throws IOException{
    getLifeCycle().checkStateAndClose(this::closeImpl);
  }

  /**
   * 调用 getProxies().close()，关闭与 Raft 节点的所有代理。
   */
  public void closeImpl() throws IOException {
    getProxies().close();
  }
}
