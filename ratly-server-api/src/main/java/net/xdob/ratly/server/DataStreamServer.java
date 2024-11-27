package net.xdob.ratly.server;

import java.io.Closeable;

/**
 * 用于流式数据传输的服务器接口，
 * 它扩展了 Closeable 接口，表示该服务器在使用完毕后需要关闭以释放资源。
 */
public interface DataStreamServer extends Closeable {
  /**
   * 此方法返回一个 DataStreamServerRpc 对象，这是一个用于流式数据传输的服务器 RPC（远程过程调用）接口。
   * 该接口提供了与客户端进行流式数据交互的功能。
   */
  DataStreamServerRpc getServerRpc();
}
