package net.xdob.ratly.client.api;

import net.xdob.ratly.protocol.BeanTarget;
import net.xdob.ratly.util.function.SerialFunction;

public interface DRpcApi {
  <T,R> R invokeRpc(SerialFunction<T,R> fun);
  <T,R> R invokeRpc(Class<T> clazz, SerialFunction<T,R> fun) ;
  <T,R> R invokeRpc(BeanTarget<T> target, SerialFunction<T,R> fun) ;
}
