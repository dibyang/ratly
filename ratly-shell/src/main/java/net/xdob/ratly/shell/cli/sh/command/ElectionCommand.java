
package net.xdob.ratly.shell.cli.sh.command;

import net.xdob.ratly.shell.cli.Command;
import net.xdob.ratly.shell.cli.sh.election.PauseCommand;
import net.xdob.ratly.shell.cli.sh.election.ResumeCommand;
import net.xdob.ratly.shell.cli.sh.election.StepDownCommand;
import net.xdob.ratly.shell.cli.sh.election.TransferCommand;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class ElectionCommand extends AbstractParentCommand {
  private static final List<Function<Context, Command>> SUB_COMMAND_CONSTRUCTORS
      = Collections.unmodifiableList(Arrays.asList(
      TransferCommand::new, StepDownCommand::new, PauseCommand::new, ResumeCommand::new));

  /**
   * @param context command context
   */
  public ElectionCommand(Context context) {
    super(context, SUB_COMMAND_CONSTRUCTORS);
  }

  @Override
  public String getCommandName() {
    return "election";
  }

  @Override
  public String getDescription() {
    return description();
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Manage ratly leader election; see the sub-commands for the details.";
  }
}
