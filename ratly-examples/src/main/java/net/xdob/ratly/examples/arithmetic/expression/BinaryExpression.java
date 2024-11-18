package net.xdob.ratly.examples.arithmetic.expression;

import net.xdob.ratly.util.Preconditions;

import java.util.Map;
import java.util.Objects;
import java.util.function.BinaryOperator;

public class BinaryExpression implements Expression {
  public enum Op implements BinaryOperator<Expression> {
    ADD("+"), SUBTRACT("-"), MULT("*"), DIV("/");

    private final String symbol;

    Op(String symbol) {
      this.symbol = symbol;
    }

    byte byteValue() {
      return (byte) ordinal();
    }

    @Override
    public BinaryExpression apply(Expression l, Expression r) {
      return new BinaryExpression(this, l, r);
    }

    public BinaryExpression apply(double l, Expression r) {
      return apply(new DoubleValue(l), r);
    }

    public BinaryExpression apply(Expression l, double r) {
      return apply(l, new DoubleValue(r));
    }

    public BinaryExpression apply(double l, double r) {
      return apply(new DoubleValue(l), new DoubleValue(r));
    }

    @Override
    public String toString() {
      return symbol;
    }

    static final Op[] VALUES = Op.values();

    static Op valueOf(byte b) {
      Preconditions.assertTrue(b < VALUES.length);
      return VALUES[b];
    }

    public String getSymbol() {
      return symbol;
    }
  }

  private final Op op;
  private final Expression left, right;

  BinaryExpression(byte[] buf, final int offset) {
    Preconditions.assertTrue(buf[offset] == Type.BINARY.byteValue());
    op = Op.valueOf(buf[offset + 1]);
    left = Utils.bytes2Expression(buf, offset + 2);
    right = Utils.bytes2Expression(buf, offset + 2 + left.length());
  }

  public BinaryExpression(Op op, Expression left, Expression right) {
    this.op = op;
    this.left = left;
    this.right = right;
  }

  @Override
  public int toBytes(byte[] buf, final int offset) {
    buf[offset] = Type.BINARY.byteValue();
    buf[offset + 1] = op.byteValue();
    final int l = left.toBytes(buf, offset + 2);
    final int r = right.toBytes(buf, offset + 2 + l);
    return 2 + l + r;
  }

  @Override
  public int length() {
    return 2 + left.length() + right.length();
  }

  @Override
  public Double evaluate(Map<String, Double> variableMap) {
    final double l = left.evaluate(variableMap);
    final double r = right.evaluate(variableMap);
    switch (op) {
      case ADD:
        return l + r;
      case SUBTRACT:
        return l - r;
      case MULT:
        return l * r;
      case DIV:
        return l / r;
      default:
        throw new AssertionError("Unexpected op value: " + op);
    }
  }

  @Override
  public String toString() {
    return "(" + left + " " + op + " " + right + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BinaryExpression that = (BinaryExpression) o;
    return op == that.op &&
        Objects.equals(left, that.left) &&
        Objects.equals(right, that.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(op);
  }
}
