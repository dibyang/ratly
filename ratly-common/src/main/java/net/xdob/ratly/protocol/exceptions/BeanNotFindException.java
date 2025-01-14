package net.xdob.ratly.protocol.exceptions;

import net.xdob.ratly.protocol.BeanTarget;

import java.io.IOException;

public class BeanNotFindException extends IOException {
  public BeanNotFindException(String message) {
    super(message);
  }

  public BeanNotFindException(BeanTarget<?> target) {
    this("Bean not find. target="+target);
  }
}
