package net.xdob.ratly.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import net.xdob.ratly.util.JavaUtils;
import net.xdob.ratly.util.LifeCycle;
import net.xdob.ratly.util.NetUtils;

import java.io.Closeable;
import java.net.InetSocketAddress;

public class NettyClient implements Closeable {
  private final LifeCycle lifeCycle;
  private final String serverAddress;
  private Channel channel;

  NettyClient(String serverAddress) {
    this.lifeCycle = new LifeCycle(JavaUtils.getClassSimpleName(getClass()) + "-" + serverAddress);
    this.serverAddress = serverAddress;
  }

  /** Connects to the given server address. */
  public void connect(EventLoopGroup group, ChannelInitializer<SocketChannel> initializer)
      throws InterruptedException {
    final InetSocketAddress address = NetUtils.createSocketAddr(serverAddress);

    lifeCycle.startAndTransition(
        () -> channel = new Bootstrap()
            .group(group)
            .channel(NettyUtils.getSocketChannelClass(group))
            .handler(new LoggingHandler(LogLevel.INFO))
            .handler(initializer)
            .connect(address)
            .sync()
            .channel(),
        InterruptedException.class);
  }

  @Override
  public void close() {
    lifeCycle.checkStateAndClose(() -> NettyUtils.closeChannel(channel, serverAddress));
  }

  public ChannelFuture writeAndFlush(Object msg) {
    lifeCycle.assertCurrentState(LifeCycle.States.RUNNING);
    return channel.writeAndFlush(msg);
  }

  @Override
  public String toString() {
    return lifeCycle.toString();
  }
}
