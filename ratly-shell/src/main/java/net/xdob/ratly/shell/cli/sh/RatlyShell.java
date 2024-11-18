
package net.xdob.ratly.shell.cli.sh;

import net.xdob.ratly.conf.Parameters;
import net.xdob.ratly.conf.RaftProperties;
import net.xdob.ratly.retry.RetryPolicy;
import net.xdob.ratly.shell.cli.AbstractShell;
import net.xdob.ratly.shell.cli.Command;
import net.xdob.ratly.shell.cli.sh.command.AbstractParentCommand;
import net.xdob.ratly.shell.cli.sh.command.Context;
import net.xdob.ratly.shell.cli.sh.command.ElectionCommand;
import net.xdob.ratly.shell.cli.sh.command.GroupCommand;
import net.xdob.ratly.shell.cli.sh.command.LocalCommand;
import net.xdob.ratly.shell.cli.sh.command.PeerCommand;
import net.xdob.ratly.shell.cli.sh.command.SnapshotCommand;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Shell for manage ratly group.
 */
public class RatlyShell extends AbstractShell {
  static final List<Function<Context, AbstractParentCommand>> PARENT_COMMAND_CONSTRUCTORS
      = Collections.unmodifiableList(Arrays.asList(
          PeerCommand::new, GroupCommand::new, ElectionCommand::new, SnapshotCommand::new, LocalCommand::new));

  static List<AbstractParentCommand> allParentCommands(Context context) {
    return PARENT_COMMAND_CONSTRUCTORS.stream()
        .map(constructor -> constructor.apply(context))
        .collect(Collectors.toList());
  }

  /**
   * Manage ratly shell command.
   *
   * @param args array of arguments given by the user's input from the terminal
   */
  public static void main(String[] args) {
    final RatlyShell shell = new RatlyShell(System.out);
    System.exit(shell.run(args));
  }

  public RatlyShell(PrintStream out) {
    this(new Context(out));
  }

  private RatlyShell(Context context) {
    super(context);
  }

  @Override
  protected String getShellName() {
    return "sh";
  }

  @Override
  protected Map<String, Command> loadCommands(Context context) {
    return allParentCommands(context).stream()
        .collect(Collectors.toMap(Command::getCommandName, Function.identity()));
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private PrintStream printStream = System.out;
    private RetryPolicy retryPolicy;
    private RaftProperties properties;
    private Parameters parameters;

    public Builder setPrintStream(PrintStream printStream) {
      this.printStream = printStream;
      return this;
    }

    public Builder setRetryPolicy(RetryPolicy retryPolicy) {
      this.retryPolicy = retryPolicy;
      return this;
    }

    public Builder setProperties(RaftProperties properties) {
      this.properties = properties;
      return this;
    }

    public Builder setParameters(Parameters parameters) {
      this.parameters = parameters;
      return this;
    }

    public RatlyShell build() {
      return new RatlyShell(new Context(printStream, false, retryPolicy, properties, parameters));
    }
  }
}
