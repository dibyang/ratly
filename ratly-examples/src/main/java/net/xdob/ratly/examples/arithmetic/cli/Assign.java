
package net.xdob.ratly.examples.arithmetic.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.examples.arithmetic.AssignmentMessage;
import net.xdob.ratly.examples.arithmetic.expression.BinaryExpression;
import net.xdob.ratly.examples.arithmetic.expression.DoubleValue;
import net.xdob.ratly.examples.arithmetic.expression.Expression;
import net.xdob.ratly.examples.arithmetic.expression.UnaryExpression;
import net.xdob.ratly.examples.arithmetic.expression.Variable;
import net.xdob.ratly.protocol.RaftClientReply;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Subcommand to assign new value in arithmetic state machine.
 */
@Parameters(commandDescription = "Assign value to a variable.")
public class Assign extends Client {

  private static final Pattern NUMBER_PATTERN = Pattern.compile("\\d+|\\d*\\.\\d+");
  private static final String VARIABLE_OR_NUMBER = String.format("(%s|%s)", NUMBER_PATTERN, Variable.PATTERN.pattern());
  private static final Pattern BINARY_OPERATION_PATTERN = Pattern.compile(
      VARIABLE_OR_NUMBER + "\\s*([*+/-])\\s*" + VARIABLE_OR_NUMBER);
  private static final Pattern UNARY_OPERATION_PATTERN = Pattern.compile("([√~-])" + VARIABLE_OR_NUMBER);

  @Parameter(names = {"--name"},
      description = "Name of the variable to set", required = true)
  private String name;

  @Parameter(names = {"--value"}, description = "Value to set", required = true)
  private String value;

  @Override
  protected void operation(RaftClient client) throws IOException {
    RaftClientReply send = client.io().send(
        new AssignmentMessage(new Variable(name), createExpression(value)));
    System.out.println("Success: " + send.isSuccess());
    System.out.println("Response: " + send.getMessage().getClass());
  }

  @VisibleForTesting
  Expression createExpression(String val) {
    if (NUMBER_PATTERN.matcher(val).matches()) {
      return new DoubleValue(Double.parseDouble(val));
    } else if (Variable.PATTERN.matcher(val).matches()) {
      return new Variable(val);
    }
    Matcher binaryMatcher = BINARY_OPERATION_PATTERN.matcher(val);
    Matcher unaryMatcher = UNARY_OPERATION_PATTERN.matcher(val);

    if (binaryMatcher.matches()) {
      return createBinaryExpression(binaryMatcher);
    } else if (unaryMatcher.matches()) {
      return createUnaryExpression(unaryMatcher);
    } else {
      throw new IllegalArgumentException("Invalid expression " + val + " Try something like: 'a+b' or '2'");
    }
  }

  private Expression createBinaryExpression(Matcher binaryMatcher) {
    String operator = binaryMatcher.group(2);
    String firstElement = binaryMatcher.group(1);
    String secondElement = binaryMatcher.group(3);
    Optional<BinaryExpression.Op> selectedOp =
        Arrays.stream(BinaryExpression.Op.values()).filter(op -> op.getSymbol().equals(operator)).findAny();

    if (!selectedOp.isPresent()) {
      throw new IllegalArgumentException("Unknown binary operator: " + operator);
    } else {
      return new BinaryExpression(selectedOp.get(), createExpression(firstElement), createExpression(secondElement));
    }
  }

  private Expression createUnaryExpression(Matcher binaryMatcher) {
    String operator = binaryMatcher.group(1);
    String element = binaryMatcher.group(2);
    Optional<UnaryExpression.Op> selectedOp =
        Arrays.stream(UnaryExpression.Op.values()).filter(op -> op.getSymbol().equals(operator)).findAny();

    if (!selectedOp.isPresent()) {
      throw new IllegalArgumentException("Unknown unary operator:" + operator);
    } else {
      return new UnaryExpression(selectedOp.get(), createExpression(element));
    }
  }

}
