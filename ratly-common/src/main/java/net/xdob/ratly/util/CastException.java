package net.xdob.ratly.util;

import java.lang.reflect.Type;

public class CastException extends RuntimeException {
  private static final long serialVersionUID = 1L;


  public CastException(Type type, Object value) {
    super(buildMessage(type, value));
  }

  static String buildMessage(Type type, Object value) {
    return "Type cast error:" + value + " can not cast " + type;
  }

  public CastException(Throwable cause, Type type, Object value) {
    super(buildMessage(type, value),cause);
  }

}
