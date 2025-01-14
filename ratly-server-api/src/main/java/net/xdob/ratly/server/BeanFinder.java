package net.xdob.ratly.server;

import net.xdob.ratly.protocol.BeanTarget;

@FunctionalInterface
public interface BeanFinder {
  <T> T getBean(BeanTarget<T> target);
}
