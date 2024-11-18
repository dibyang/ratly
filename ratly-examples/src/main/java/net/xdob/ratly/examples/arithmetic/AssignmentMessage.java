package net.xdob.ratly.examples.arithmetic;

import static net.xdob.ratly.util.ProtoUtils.toByteString;

import java.nio.charset.Charset;
import java.util.Map;

import net.xdob.ratly.examples.arithmetic.expression.Expression;
import net.xdob.ratly.examples.arithmetic.expression.Variable;
import net.xdob.ratly.protocol.Message;
import com.google.protobuf.ByteString;

public class AssignmentMessage implements Message, Evaluable {
  public static final Charset UTF8 = Charset.forName("UTF-8");

  private final Variable variable;
  private final Expression expression;

  public AssignmentMessage(Variable variable, Expression expression) {
    this.variable = variable;
    this.expression = expression;
  }

  public AssignmentMessage(byte[] buf, int offset) {
    variable = new Variable(buf, offset);
    expression = Expression.Utils.bytes2Expression(buf, offset + variable.length());
  }

  public AssignmentMessage(ByteString bytes) {
    this(bytes.toByteArray(), 0);
  }

  public Variable getVariable() {
    return variable;
  }

  public Expression getExpression() {
    return expression;
  }

  @Override
  public ByteString getContent() {
    final int length = variable.length() + expression.length();
    final byte[] bytes = new byte[length];
    final int offset = variable.toBytes(bytes, 0);
    expression.toBytes(bytes, offset);
    return toByteString(bytes);
  }

  @Override
  public String toString() {
    return variable + " = " + expression;
  }

  @Override
  public Double evaluate(Map<String, Double> variableMap) {
    final Double value = expression.evaluate(variableMap);
    final String name = variable.getName();
    if (value == null) {
      variableMap.remove(name);
    } else {
      variableMap.put(name, value);
    }
    return value;
  }
}
