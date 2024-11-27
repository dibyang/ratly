package net.xdob.ratly.server;

import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.datastream.DataStreamFactory;
import net.xdob.ratly.datastream.DataStreamType;

/**
 * 接口继承自 DataStreamFactory，用于创建服务器端的流式数据传输相关对象，特别是 DataStreamServerRpc 实现。
 * 它提供了一个工厂方法 newDataStreamServerRpc 来创建新的 DataStreamServerRpc 实例，
 * 并包含一个静态方法 newInstance 用于根据 DataStreamType 和 Parameters 创建相应的 DataStreamServerFactory 实例。
 * A {@link DataStreamFactory} to create server-side objects. */
public interface DataStreamServerFactory extends DataStreamFactory {
  /**
   * 该静态方法根据提供的 DataStreamType 和 Parameters，创建一个 DataStreamServerFactory 实例。
   * 首先，调用 DataStreamType 的 newServerFactory 方法来获取一个 DataStreamFactory 实例。
   * 如果这个实例是 DataStreamServerFactory 类型，则返回它，否则抛出一个 ClassCastException 异常。
   */
  static DataStreamServerFactory newInstance(DataStreamType type, Parameters parameters) {
    final DataStreamFactory dataStreamFactory = type.newServerFactory(parameters);
    if (dataStreamFactory instanceof DataStreamServerFactory) {
      return (DataStreamServerFactory)dataStreamFactory;
    }
    throw new ClassCastException("Cannot cast " + dataStreamFactory.getClass()
        + " to " + DataStreamServerFactory.class + "; rpc type is " + type);
  }

  /**
   * 该方法接受一个 RaftServer 实例，并返回一个  {@link DataStreamServerRpc} 实例。
   * {@link DataStreamServerRpc} 是服务器端与客户端之间进行数据流通信的接口。
   */
  DataStreamServerRpc newDataStreamServerRpc(RaftServer server);
}
