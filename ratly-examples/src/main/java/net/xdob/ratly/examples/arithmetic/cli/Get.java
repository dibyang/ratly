package net.xdob.ratly.examples.arithmetic.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import net.xdob.ratly.client.RaftClient;
import net.xdob.ratly.examples.arithmetic.expression.DoubleValue;
import net.xdob.ratly.examples.arithmetic.expression.Expression;
import net.xdob.ratly.examples.arithmetic.expression.Variable;
import net.xdob.ratly.protocol.RaftClientReply;

import java.io.IOException;

/**
 * Subcommand to get value from the state machine.
 */
@Parameters(commandDescription = "Assign value to a variable.")
public class Get extends Client {

  @Parameter(names = {
      "--name"}, description = "Name of the variable to set", required = true)
  private String name;

  @Override
  protected void operation(RaftClient client) throws IOException {
    RaftClientReply getValue =
        client.io().sendReadOnly(Expression.Utils.toMessage(new Variable(name)));
    Expression response =
        Expression.Utils.bytes2Expression(getValue.getMessage().getContent().toByteArray(), 0);
    System.out.println(String.format("%s=%s", name, (DoubleValue) response).toString());
  }
}
