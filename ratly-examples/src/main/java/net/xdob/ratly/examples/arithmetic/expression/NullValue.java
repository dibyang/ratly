package net.xdob.ratly.examples.arithmetic.expression;

import net.xdob.ratly.util.Preconditions;

import java.util.Map;

public final class NullValue implements Expression {
  private static final NullValue INSTANCE = new NullValue();

  public static NullValue getInstance() {
    return INSTANCE;
  }

  private NullValue() {
  }

  @Override
  public int toBytes(byte[] buf, int offset) {
    Preconditions.assertTrue(offset + length() <= buf.length);
    buf[offset] = Type.NULL.byteValue();
    return length();
  }

  @Override
  public int length() {
    return 1;
  }

  @Override
  public Double evaluate(Map<String, Double> variableMap) {
    return null;
  }

  @Override
  public String toString() {
    return "null";
  }
}
