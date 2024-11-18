
package net.xdob.ratly.shell.cli.sh.command;

import net.xdob.ratly.shell.cli.Command;
import net.xdob.ratly.shell.cli.sh.local.RaftMetaConfCommand;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Command for local operation, which no need to connect to ratly server
 */
public class LocalCommand extends AbstractParentCommand {

  private static final List<Function<Context, Command>> SUB_COMMAND_CONSTRUCTORS
      = Collections.unmodifiableList(Arrays.asList(RaftMetaConfCommand::new));

  /**
   * @param context command context
   */
  public LocalCommand(Context context) {
    super(context, SUB_COMMAND_CONSTRUCTORS);
  }

  @Override
  public String getCommandName() {
    return "local";
  }

  @Override
  public String getDescription() {
    return description();
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Command for local operation, which no need to connect to ratly server; "
        + "see the sub-commands for the details.";
  }
}
